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
package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.HashMap;
import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveFileFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.library.vertexmanager.ShuffleVertexManager;
import org.apache.tez.runtime.library.cartesianproduct.CartesianProductVertexManager;
import org.mockito.ArgumentCaptor;
import org.junit.Test;

import org.junit.Assert;

/**
 * 
 */
public class TestDagUtils {

  @Test
  public void testCredentialsNotOverwritten() throws Exception {
    final UserGroupInformation testUser = UserGroupInformation.createUserForTesting("test_user", new String[0]);
    final DagUtils dagUtils = DagUtils.getInstance();

    Credentials originalCredentials = new Credentials();
    final Text testTokenAlias = new Text("my_test_token");
    @SuppressWarnings("unchecked")
    Token<? extends TokenIdentifier> testToken = mock(Token.class);
    originalCredentials.addToken(testTokenAlias, testToken);
    Credentials testUserCredentials = new Credentials();

    testUser.addCredentials(testUserCredentials);

    final BaseWork work = mock(BaseWork.class);
    final DAG dag = DAG.create("test_credentials_dag");

    dag.setCredentials(originalCredentials);

    testUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        dagUtils.addCredentials(work, dag, null);
        return null;
      }
    });

    Token<? extends TokenIdentifier> actualToken = dag.getCredentials().getToken(testTokenAlias);
    assertEquals(testToken, actualToken);
  }

  @Test
  public void outputCommitterSetToDefaultIfNotPresent() throws IOException {
    DagUtils dagUtils = DagUtils.getInstance();
    HiveConf conf = new HiveConf();

    JobConf configuration = dagUtils.createConfiguration(conf);

    assertEquals(HiveFileFormatUtils.NullOutputCommitter.class.getName(),
        configuration.get("mapred.output.committer.class"));
  }

  @Test
  public void outputCommitterNotOverriddenIfPresent() throws IOException {
    DagUtils dagUtils = DagUtils.getInstance();
    HiveConf conf = new HiveConf();
    conf.set("mapred.output.committer.class", TestTezOutputCommitter.CountingOutputCommitter.class.getName());

    JobConf configuration = dagUtils.createConfiguration(conf);

    assertEquals(TestTezOutputCommitter.CountingOutputCommitter.class.getName(),
        configuration.get("mapred.output.committer.class"));
  }

  @Test
  public void testMapTezTaskEnvIsCopiedFromMrProperties() {
    final DagUtils dagUtils = DagUtils.getInstance();

    Vertex map = Vertex.create("mapWorkName", null);
    HiveConf conf = new HiveConf();
    Assert.assertNull(map.getTaskEnvironment().get("key"));

    conf.set(JobConf.MAPRED_MAP_TASK_ENV, "key=value");
    map.setTaskEnvironment(dagUtils.getContainerEnvironment(conf, true));

    Assert.assertEquals("value", map.getTaskEnvironment().get("key"));
  }

  @Test
  public void testReduceTezTaskEnvIsCopiedFromMrProperties() {
    final DagUtils dagUtils = DagUtils.getInstance();

    Vertex reduce = Vertex.create("reduceWorkName", null);
    HiveConf conf = new HiveConf();
    Assert.assertNull(reduce.getTaskEnvironment().get("key"));

    conf.set(JobConf.MAPRED_REDUCE_TASK_ENV, "key=value");
    reduce.setTaskEnvironment(dagUtils.getContainerEnvironment(conf, false));

    Assert.assertEquals("value", reduce.getTaskEnvironment().get("key"));
  }

  /**
   * The LLAP floor fixed a reduce-side merge join's main work at 96 tasks: the vertex must not auto-reduce, though both
   * its inputs are shuffles (TPC-DS q51 ran 14 tasks while its vertex manager came from the second input's edge).
   */
  @Test
  public void testMergeJoinFixedByItsMainWorkDoesNotAutoReduce() throws Exception {
    ReduceWork main = new ReduceWork("Reducer 3");
    main.setNumReduceTasks(96);
    TezWork tezWork = new TezWork("q");
    MergeJoinWork mergeJoin = mergeJoin(tezWork, main);
    connect(tezWork, mergeJoin, EdgeType.SIMPLE_EDGE, EdgeType.SIMPLE_EDGE);

    Vertex vertex = mock(Vertex.class);
    DagUtils.getInstance().setupVertexManager(new JobConf(), vertex, mergeJoin, tezWork);

    verify(vertex, never()).setVertexManagerPlugin(any());
  }

  /** An auto-reduced merge join auto-reduces from its main work's minimum. */
  @Test
  public void testMergeJoinAutoReducesFromItsMainWork() throws Exception {
    ReduceWork main = new ReduceWork("Reducer 3");
    main.setAutoReduceParallelism(true);
    main.setMinReduceTasks(96);
    main.setMaxReduceTasks(104);
    main.setMinSrcFraction(0.25f);
    main.setMaxSrcFraction(0.75f);
    TezWork tezWork = new TezWork("q");
    MergeJoinWork mergeJoin = mergeJoin(tezWork, main);
    connect(tezWork, mergeJoin, EdgeType.SIMPLE_EDGE, EdgeType.SIMPLE_EDGE);
    JobConf conf = new JobConf();
    conf.setLong(HiveConf.ConfVars.BYTES_PER_REDUCER.varname, 256_000_000L);

    VertexManagerPluginDescriptor plugin = vertexManager(conf, mergeJoin, tezWork);

    Assert.assertEquals(ShuffleVertexManager.class.getName(), plugin.getClassName());
    Configuration pluginConf = TezUtils.createConfFromUserPayload(plugin.getUserPayload());
    Assert.assertTrue(pluginConf.getBoolean(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_ENABLE_AUTO_PARALLEL, false));
    Assert.assertEquals(96, pluginConf.getInt(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_TASK_PARALLELISM, -1));
    Assert.assertEquals(256_000_000L,
        pluginConf.getLong(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_DESIRED_TASK_INPUT_SIZE, -1));
    Assert.assertEquals(0.25f, pluginConf.getFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, -1), 0);
    Assert.assertEquals(0.75f, pluginConf.getFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, -1), 0);
  }

  /** A bucket map join's CUSTOM_EDGE routes buckets: CustomPartitionVertex, though the work auto-reduces its shuffle. */
  @Test
  public void testCustomEdgeKeepsCustomPartitionVertex() throws Exception {
    ReduceWork work = new ReduceWork("Reducer 2");
    work.setAutoReduceParallelism(true);
    TezWork tezWork = new TezWork("q");
    tezWork.add(work);
    connect(tezWork, work, EdgeType.SIMPLE_EDGE, EdgeType.CUSTOM_EDGE);
    tezWork.setVertexType(work, TezWork.VertexType.INITIALIZED_EDGES);

    Assert.assertEquals(CustomPartitionVertex.class.getName(),
        vertexManager(new JobConf(), work, tezWork).getClassName());
  }

  /** A cross product needs CartesianProductVertexManager, whatever its other inputs. */
  @Test
  public void testCrossProductKeepsCartesianProductVertexManager() throws Exception {
    ReduceWork main = new ReduceWork("Reducer 3");
    main.setAutoReduceParallelism(true);
    TezWork tezWork = new TezWork("q");
    MergeJoinWork mergeJoin = mergeJoin(tezWork, main);
    connect(tezWork, mergeJoin, EdgeType.XPROD_EDGE, EdgeType.XPROD_EDGE, EdgeType.SIMPLE_EDGE);

    Assert.assertEquals(CartesianProductVertexManager.class.getName(),
        vertexManager(new JobConf(), mergeJoin, tezWork).getClassName());
  }

  /** A work that does not wait for its sources (a semijoin bloom filter merge) starts without slow start. */
  @Test
  public void testQuickStart() throws Exception {
    ReduceWork work = new ReduceWork("Reducer 2");
    work.setSlowStart(false);
    TezWork tezWork = new TezWork("q");
    tezWork.add(work);
    connect(tezWork, work, EdgeType.CUSTOM_SIMPLE_EDGE);

    VertexManagerPluginDescriptor plugin = vertexManager(new JobConf(), work, tezWork);

    Assert.assertEquals(ShuffleVertexManager.class.getName(), plugin.getClassName());
    Configuration pluginConf = TezUtils.createConfFromUserPayload(plugin.getUserPayload());
    Assert.assertEquals(0, pluginConf.getFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MIN_SRC_FRACTION, -1), 0);
    Assert.assertEquals(0, pluginConf.getFloat(ShuffleVertexManager.TEZ_SHUFFLE_VERTEX_MANAGER_MAX_SRC_FRACTION, -1), 0);
  }

  /** A partitioned input of a work that waits for its sources keeps Tez's default slow start. */
  @Test
  public void testSlowStartKeepsTheDefaultVertexManager() throws Exception {
    ReduceWork work = new ReduceWork("Reducer 2");
    TezWork tezWork = new TezWork("q");
    tezWork.add(work);
    connect(tezWork, work, EdgeType.CUSTOM_SIMPLE_EDGE);

    Vertex vertex = mock(Vertex.class);
    DagUtils.getInstance().setupVertexManager(new JobConf(), vertex, work, tezWork);

    verify(vertex, never()).setVertexManagerPlugin(any());
  }

  /**
   * Only a shuffle input auto-reduces: the partitioned inputs of a dynamically partitioned hash join keep the partition
   * count both sides were hashed into.
   */
  @Test
  public void testPartitionedInputsDoNotAutoReduce() throws Exception {
    ReduceWork work = new ReduceWork("Reducer 2");
    work.setAutoReduceParallelism(true);
    TezWork tezWork = new TezWork("q");
    tezWork.add(work);
    connect(tezWork, work, EdgeType.CUSTOM_SIMPLE_EDGE, EdgeType.CUSTOM_SIMPLE_EDGE);

    Vertex vertex = mock(Vertex.class);
    DagUtils.getInstance().setupVertexManager(new JobConf(), vertex, work, tezWork);

    verify(vertex, never()).setVertexManagerPlugin(any());
  }

  private static MergeJoinWork mergeJoin(TezWork tezWork, ReduceWork main) {
    MergeJoinWork mergeJoin = new MergeJoinWork();
    mergeJoin.addMergedWork(main, null, new HashMap<>());
    tezWork.add(mergeJoin);
    return mergeJoin;
  }

  /** Connects one parent work per edge type to the given work. */
  private static void connect(TezWork tezWork, BaseWork work, EdgeType... edgeTypes) {
    for (int i = 0; i < edgeTypes.length; i++) {
      ReduceWork parent = new ReduceWork("Reducer " + (10 + i));
      tezWork.add(parent);
      tezWork.connect(parent, work, new TezEdgeProperty(edgeTypes[i], edgeTypes[i] == EdgeType.CUSTOM_EDGE ? 8 : -1));
    }
  }

  private static VertexManagerPluginDescriptor vertexManager(JobConf conf, BaseWork work, TezWork tezWork)
      throws IOException {
    Vertex vertex = mock(Vertex.class);
    DagUtils.getInstance().setupVertexManager(conf, vertex, work, tezWork);
    ArgumentCaptor<VertexManagerPluginDescriptor> plugin = ArgumentCaptor.forClass(VertexManagerPluginDescriptor.class);
    verify(vertex).setVertexManagerPlugin(plugin.capture());
    return plugin.getValue();
  }
}
