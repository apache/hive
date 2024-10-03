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

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.HiveSplitGenerator.SplitSerializer;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat;
import org.apache.hadoop.hive.ql.io.ZeroRowsInputFormat;
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
import org.apache.tez.mapreduce.protos.MRRuntimeProtos.MRSplitProto;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHiveSplitGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(TestHiveSplitGenerator.class);

  private static final String QUERY_ID = "hive_" + System.currentTimeMillis();
  private static final ApplicationId APP_ID = ApplicationId.newInstance(1000, 200);
  private static final String INPUT_NAME = "MRInput";
  private static final int VERTEX_ID = 0;
  private static final String PATH_FORMAT = "events/%s/%d_%s_InputDataInformationEvent_%d";

  @Test
  public void testSplitSerializationUsingFileSystem() throws Exception {
    JobConf conf = new JobConf(new HiveConf());
    // 0 bytes will force a split serialization to filesystem
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TEZ_INPUT_FS_SERIALIZATION_THRESHOLD, 0);

    InputSplit[] splits = getSplits(1);
    String serializedPath = ((InputDataInformationEvent) generateEvents(conf, splits).get(1)).getSerializedPath();
    // second split is serialized
    Assert.assertTrue(
        "Serialized path should contain a pattern like "
            + "'events/$queryid/$vertexid_$inputname_InputDataInformationEvent_$count', but found: " + serializedPath,
        serializedPath.contains(expectedSerializedPathFormat(0)));
  }

  @Test
  public void testSplitSerializationInMemoryLimitExceeded() throws Exception {
    JobConf conf = new JobConf(new HiveConf());
    // split size is around 200 bytes, so first one will be held in memory, second will be serialized (200 > 10)
    // this is the most typical usecase of the split fs serialization feature
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TEZ_INPUT_FS_SERIALIZATION_THRESHOLD, 10);

    InputSplit[] splits = getSplits(2);
    List<Event> events = generateEvents(conf, splits);
    String serializedPath1 = ((InputDataInformationEvent) events.get(1)).getSerializedPath();
    String serializedPath2 = ((InputDataInformationEvent) events.get(2)).getSerializedPath();

    // first split is held in memory, no serialized path found
    Assert.assertNull(serializedPath1);

    // second split is serialized
    Assert.assertTrue(
        "Serialized path should contain a pattern like "
            + "'events/$queryid/$vertexid_$inputname_InputDataInformationEvent_$count', but found: " + serializedPath2,
        serializedPath2.contains(expectedSerializedPathFormat(1)));
  }

  @Test
  public void testSplitSerializationNoFileSystemBecauseOfDefaultConfig() throws Exception {
    JobConf conf = new JobConf(new HiveConf());

    InputSplit[] splits = getSplits(1);
    String serializedPath = ((InputDataInformationEvent) generateEvents(conf, splits).get(1)).getSerializedPath();
    // the default value 512KB is to large to trigger split serialization to filesystem
    Assert.assertNull(serializedPath);
  }

  @Test
  public void testSplitSerializationDisabledFilesystem() throws Exception {
    JobConf conf = new JobConf(new HiveConf());
    // -1 disables the split serialization to filesystem
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TEZ_INPUT_FS_SERIALIZATION_THRESHOLD, -1);

    InputSplit[] splits = getSplits(1);
    String serializedPath = ((InputDataInformationEvent) generateEvents(conf, splits).get(1)).getSerializedPath();
    Assert.assertNull(serializedPath);
  }

  @Test
  public void testExceptionIsPropagatedFromSplitSerializer() throws Exception {
    JobConf conf = new JobConf(new HiveConf());
    HiveConf.setIntVar(conf, HiveConf.ConfVars.HIVE_TEZ_INPUT_FS_SERIALIZATION_THRESHOLD, 0);

    InputSplit[] splits = getSplits(3);
    InputInitializerContext context = getInputInitializerContext(conf);
    HiveSplitGeneratorSerializerException splitGenerator = new HiveSplitGeneratorSerializerException(context);
    splitGenerator.prepare(context);
    try {
      String serializedPath =
          ((InputDataInformationEvent) generateEvents(conf, splits, splitGenerator).get(1)).getSerializedPath();
      Assert.fail("HiveSplitGeneratorSerializerException should fail");
    } catch (Exception e) {
      // outermost exception is the RuntimeException that wraps anything comes from splitSerializer.close()
      Assert.assertEquals(RuntimeException.class, e.getClass());

      // wrapperRuntimeException wraps anything thrown from splitSerializer.writeSplit()
      Throwable wrapperRuntimeException = e.getCause();
      Assert.assertEquals(RuntimeException.class, wrapperRuntimeException.getClass());

      Throwable originalException = wrapperRuntimeException.getCause();
      Assert.assertEquals(IOException.class, originalException.getClass());
      Assert
          .assertTrue(originalException.getMessage().contains(HiveSplitGeneratorSerializerException.EXCEPTION_MESSAGE));

      Assert.assertTrue("Already running future in not supposed to be cancelled with the current implementation",
          splitGenerator.split0Finished.get());
      Assert.assertFalse("A future started after a failure is not supposed to run at all",
          splitGenerator.split2Finished.get());
    }
  }

  private InputSplit[] getSplits(int numSplits) {
    InputSplit[] splits = new InputSplit[numSplits];

    for (int i = 0; i < numSplits; i++) {
      splits[i] =
          new HiveInputSplit(new NullRowsInputFormat.DummyInputSplit("/fake" + i), ZeroRowsInputFormat.class.getName());
    }
    return splits;
  }

  private String expectedSerializedPathFormat(int count) {
    return String.format(PATH_FORMAT, QUERY_ID, VERTEX_ID, INPUT_NAME, count);
  }

  private List<Event> generateEvents(JobConf conf, InputSplit[] splits) throws Exception {
    return generateEvents(conf, splits, null);
  }

  private List<Event> generateEvents(JobConf conf, InputSplit[] splits, HiveSplitGenerator splitGenerator)
      throws Exception {
    if (splitGenerator == null) {
      InputInitializerContext inputInitializerContext = getInputInitializerContext(conf);
      splitGenerator = new HiveSplitGenerator(inputInitializerContext);
      splitGenerator.prepare(inputInitializerContext);
    }

    InputSplitInfoMem splitInfo = new InputSplitInfoMem(splits, null, 1, null, conf);
    return splitGenerator.createEventList(true, splitInfo);
  }

  private InputInitializerContext getInputInitializerContext(JobConf conf) {
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.set("_hive.hdfs.session.path", "/tmp");
    conf.set("_hive.local.session.path", "/tmp");

    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID, QUERY_ID);

    MapWork mapWork = new MapWork("Map1");
    mapWork.deriveLlap(conf, false);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "file:/tmp");
    Utilities.setMapWork(conf, mapWork);

    DataSourceDescriptor dataSource = MRInput.createConfigBuilder(conf, HiveInputFormat.class).build();
    UserPayload userPayload = dataSource.getInputDescriptor().getUserPayload();
    return new InputInitializerContextForTest(userPayload, conf);
  }

  public static class HiveSplitGeneratorSerializerException extends HiveSplitGenerator {
    private static final String EXCEPTION_MESSAGE = "Cannot write file to path";
    private final AtomicBoolean split0Finished = new AtomicBoolean(false);
    private final AtomicBoolean split2Finished = new AtomicBoolean(false);

    class SplitSerializerWithException extends SplitSerializer {
      SplitSerializerWithException() throws IOException {
        super();
      }

      @Override
      InputDataInformationEvent write(int count, MRSplitProto mrSplit) {
        // mimic the following scenario: split #2 starts to be processed after split #1 failed,
        // so it won't run at all due to anyTaskFailed check
        if (count == 2) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return super.write(count, mrSplit);
      }

      @Override
      void writeSplit(int count, MRSplitProto mrSplit, Path filePath) throws IOException {
        // first split write takes longer time, started before the failing task
        // current implementation of the waitFor doesn't cancel it
        LOG.info("Write split #{}", count);
        if (count == 0) {
          try {
            Thread.sleep(1000);
            split0Finished.set(true);
            LOG.info("Split #0 finished");
          } catch (InterruptedException e) {
            LOG.info("Split #0 catches InterruptedException, this is not supposed to happen");
            throw new IOException(e);
          }
        }
        // writing second split fails
        if (count == 1) {
          LOG.info("Split #1 is about to throw exception");
          throw new IOException(EXCEPTION_MESSAGE + ": " + filePath);
        }

        // we're not supposed to reach this due to anyTaskFailed check
        if (count == 2) {
          split2Finished.set(true);
          LOG.info("Split #2 finished");
        }
      }
    }

    public HiveSplitGeneratorSerializerException(InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    @Override
    SplitSerializer getSplitSerializer() throws IOException {
      return new SplitSerializerWithException();
    }
  }

  public static class InputInitializerContextForTest implements InputInitializerContext {

    private final UserPayload payload;
    private final Configuration vertexConfig;

    public InputInitializerContextForTest(UserPayload payload, Configuration vertexConfig) {
      this.payload = payload;
      this.vertexConfig = vertexConfig;
    }

    @Override
    public ApplicationId getApplicationId() {
      return APP_ID;
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
