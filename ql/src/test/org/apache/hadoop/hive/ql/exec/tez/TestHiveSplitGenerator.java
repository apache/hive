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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat;
import org.apache.hadoop.hive.ql.io.ZeroRowsInputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.event.VertexState;
import org.apache.tez.mapreduce.hadoop.InputSplitInfoMem;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputConfigureVertexTasksEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveSplitGenerator {

  private static final String QUERY_ID = "hive_" + System.currentTimeMillis();
  private static final String INPUT_NAME = "MRInput";
  private static final int VERTEX_ID = 0;

  @Test
  public void testSplitSerializationUsingFileSystem() throws Exception {
    JobConf conf = new JobConf(new HiveConf());
    // 10 bytes will trigger a split serialization to filesystem
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TEZ_SPLIT_FS_SERIALIZATION_THRESHOLD, 10);

    String serializedPath = runSplitGeneration(conf);
    Assert.assertTrue(
        "Serialized path should contain a pattern like 'events/queryid/vertexid_inputname_InputDataInformationEvent_0'",
        serializedPath
            .contains(String.format("events/%s/%d_%s_InputDataInformationEvent_0", QUERY_ID, VERTEX_ID, INPUT_NAME)));
  }

  @Test
  public void testSplitSerializationNoFileSystemBecauseOfDefaultConfig() throws Exception {
    JobConf conf = new JobConf(new HiveConf());

    String serializedPath = runSplitGeneration(conf);
    // the default value 512KB is to large to trigger split serialization to filesystem
    Assert.assertNull(serializedPath);
  }

  @Test
  public void testSplitSerializationDisabledFilesystem() throws Exception {
    JobConf conf = new JobConf(new HiveConf());
    // -1 disables the split serialization to filesystem
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TEZ_SPLIT_FS_SERIALIZATION_THRESHOLD, -1);

    String serializedPath = runSplitGeneration(conf);
    Assert.assertNull(serializedPath);
  }

  private String runSplitGeneration(JobConf conf) throws Exception {
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.set("_hive.hdfs.session.path", "/tmp");
    conf.set("_hive.local.session.path", "/tmp");

    HiveConf.setVar(conf, HiveConf.ConfVars.HIVEQUERYID, QUERY_ID);

    MapWork mapWork = new MapWork("Map1");
    mapWork.deriveLlap(conf, false);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "file:/tmp");
    Utilities.setMapWork(conf, mapWork);

    Context ctx = new Context(conf);
    conf = DagUtils.getInstance().initializeVertexConf(conf, ctx, mapWork);

    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(conf, HiveInputFormat.class).build();
    UserPayload userPayload = dataSource.getInputDescriptor().getUserPayload();
    InputInitializerContext inputInitializerContext = new InputInitializerContextForTest(userPayload, conf);
    HiveSplitGenerator splitGenerator = new HiveSplitGenerator(inputInitializerContext);

    InputSplit[] splits = new InputSplit[1];
    splits[0] =
        new HiveInputSplit(new NullRowsInputFormat.DummyInputSplit("/fake"), ZeroRowsInputFormat.class.getName());

    InputSplitInfoMem splitInfo = new InputSplitInfoMem(splits, null, 1, null, conf);
    List<Event> events = splitGenerator.createEventList(true, splitInfo);

    // events[0] is always an InputConfigureVertexTasksEvent, so we're interested in events[1]
    return ((InputDataInformationEvent) events.get(1)).getSerializedPath();
  }

  public static class InputInitializerContextForTest implements InputInitializerContext {

    private final ApplicationId appId;
    private final UserPayload payload;
    private final Configuration vertexConfig;

    public InputInitializerContextForTest(UserPayload payload, Configuration vertexConfig) {
      appId = ApplicationId.newInstance(1000, 200);
      this.payload = payload;
      this.vertexConfig = vertexConfig;
    }

    @Override
    public ApplicationId getApplicationId() {
      return appId;
    }

    @Override
    public String getDAGName() {
      return "FakeDAG";
    }

    @Override
    public Configuration getVertexConfiguration() {
      return vertexConfig;
    }

    @Override
    public String getInputName() {
      return INPUT_NAME;
    }

    @Override
    public UserPayload getInputUserPayload() {
      return payload;
    }

    @Override
    public int getNumTasks() {
      return 100;
    }

    @Override
    public Resource getVertexTaskResource() {
      return Resource.newInstance(1024, 1);
    }

    @Override
    public int getVertexId() {
      return VERTEX_ID;
    }

    @Override
    public Resource getTotalAvailableResource() {
      return Resource.newInstance(10240, 10);
    }

    @Override
    public int getNumClusterNodes() {
      return 10;
    }

    @Override
    public int getDAGAttemptNumber() {
      return 1;
    }

    @Override
    public int getVertexNumTasks(String vertexName) {
      throw new UnsupportedOperationException("getVertexNumTasks not implemented in this mock");
    }

    @Override
    public void registerForVertexStateUpdates(String vertexName, Set<VertexState> stateSet) {
      throw new UnsupportedOperationException("getVertexNumTasks not implemented in this mock");
    }

    @Override
    public void addCounters(TezCounters tezCounters) {
      throw new UnsupportedOperationException("addCounters not implemented in this mock");
    }

    @Override
    public UserPayload getUserPayload() {
      throw new UnsupportedOperationException("getUserPayload not implemented in this mock");
    }

  }
}
