/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Resource;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    utils = mock(DagUtils.class);
    fs = mock(FileSystem.class);
    path = mock(Path.class);
    when(path.getFileSystem(any(Configuration.class))).thenReturn(fs);
    when(utils.getTezDir(any(Path.class))).thenReturn(path);
    when(
        utils.createVertex(any(JobConf.class), any(BaseWork.class), any(Path.class),
            any(LocalResource.class), any(List.class), any(FileSystem.class), any(Context.class),
            anyBoolean(), any(TezWork.class), any(VertexType.class))).thenAnswer(
        new Answer<Vertex>() {

          @Override
          public Vertex answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            return Vertex.create(((BaseWork)args[1]).getName(),
                mock(ProcessorDescriptor.class), 0, mock(Resource.class));
          }
        });

    when(utils.createEdge(any(JobConf.class), any(Vertex.class), any(Vertex.class),
            any(TezEdgeProperty.class), any(VertexType.class))).thenAnswer(new Answer<Edge>() {

          @Override
          public Edge answer(InvocationOnMock invocation) throws Throwable {
            Object[] args = invocation.getArguments();
            return Edge.create((Vertex)args[1], (Vertex)args[2], mock(EdgeProperty.class));
          }
        });

    work = new TezWork("");

    mws = new MapWork[] { new MapWork(), new MapWork()};
    rws = new ReduceWork[] { new ReduceWork(), new ReduceWork() };

    work.addAll(mws);
    work.addAll(rws);

    int i = 0;
    for (BaseWork w: work.getAllWork()) {
      w.setName("Work "+(++i));
    }

    op = mock(Operator.class);

    LinkedHashMap<String, Operator<? extends OperatorDesc>> map
      = new LinkedHashMap<String,Operator<? extends OperatorDesc>>();
    map.put("foo", op);
    mws[0].setAliasToWork(map);
    mws[1].setAliasToWork(map);

    LinkedHashMap<String, ArrayList<String>> pathMap
      = new LinkedHashMap<String, ArrayList<String>>();
    ArrayList<String> aliasList = new ArrayList<String>();
    aliasList.add("foo");
    pathMap.put("foo", aliasList);

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

    conf = new JobConf();
    appLr = mock(LocalResource.class);

    SessionState.start(new HiveConf());
    session = mock(TezClient.class);
    sessionState = mock(TezSessionState.class);
    when(sessionState.getSession()).thenReturn(session);
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
  public void testBuildDag() throws IllegalArgumentException, IOException, Exception {
    DAG dag = task.build(conf, work, path, appLr, null, new Context(conf));
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
  public void testEmptyWork() throws IllegalArgumentException, IOException, Exception {
    DAG dag = task.build(conf, new TezWork(""), path, appLr, null, new Context(conf));
    assertEquals(dag.getVertices().size(), 0);
  }

  @Test
  public void testSubmit() throws Exception {
    DAG dag = DAG.create("test");
    task.submit(conf, dag, path, appLr, sessionState, Collections.<LocalResource> emptyList(),
        new String[0], Collections.<String,LocalResource> emptyMap());
    // validate close/reopen
    verify(sessionState, times(1)).open(any(HiveConf.class), any(String[].class));
    verify(sessionState, times(1)).close(eq(true)); // now uses pool after HIVE-7043
    verify(session, times(2)).submitDAG(any(DAG.class));
  }

  @Test
  public void testClose() throws HiveException {
    task.close(work, 0);
    verify(op, times(4)).jobClose(any(Configuration.class), eq(true));
  }

  @Test
  public void testExistingSessionGetsStorageHandlerResources() throws Exception {
    final String[] inputOutputJars = new String[] {"file:///tmp/foo.jar"};
    LocalResource res = mock(LocalResource.class);
    final List<LocalResource> resources = Collections.singletonList(res);
    final Map<String,LocalResource> resMap = new HashMap<String,LocalResource>();
    resMap.put("foo.jar", res);

    when(utils.localizeTempFiles(path.toString(), conf, inputOutputJars))
        .thenReturn(resources);
    when(utils.getBaseName(res)).thenReturn("foo.jar");
    when(sessionState.isOpen()).thenReturn(true);
    when(sessionState.hasResources(inputOutputJars)).thenReturn(false);
    task.updateSession(sessionState, conf, path, inputOutputJars, resMap);
    verify(session).addAppMasterLocalFiles(resMap);
  }

  @Test
  public void testExtraResourcesAddedToDag() throws Exception {
    final String[] inputOutputJars = new String[] {"file:///tmp/foo.jar"};
    LocalResource res = mock(LocalResource.class);
    final List<LocalResource> resources = Collections.singletonList(res);
    final Map<String,LocalResource> resMap = new HashMap<String,LocalResource>();
    resMap.put("foo.jar", res);
    DAG dag = mock(DAG.class);

    when(utils.localizeTempFiles(path.toString(), conf, inputOutputJars))
        .thenReturn(resources);
    when(utils.getBaseName(res)).thenReturn("foo.jar");
    when(sessionState.isOpen()).thenReturn(true);
    when(sessionState.hasResources(inputOutputJars)).thenReturn(false);
    task.addExtraResourcesToDag(sessionState, dag, inputOutputJars, resMap);
    verify(dag).addTaskLocalFiles(resMap);
  }

  @Test
  public void testGetExtraLocalResources() throws Exception {
    final String[] inputOutputJars = new String[] {"file:///tmp/foo.jar"};
    LocalResource res = mock(LocalResource.class);
    final List<LocalResource> resources = Collections.singletonList(res);
    final Map<String,LocalResource> resMap = new HashMap<String,LocalResource>();
    resMap.put("foo.jar", res);

    when(utils.localizeTempFiles(path.toString(), conf, inputOutputJars))
        .thenReturn(resources);
    when(utils.getBaseName(res)).thenReturn("foo.jar");

    assertEquals(resMap, task.getExtraLocalResources(conf, path, inputOutputJars));
  }
}
