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

package org.apache.hadoop.hive.ql.parse.repl.load.message;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.load.MetaData;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.hive.ql.parse.repl.load.message.CreateFunctionHandler.PrimaryToReplicaResourceFunction;
import static org.apache.hadoop.hive.ql.parse.repl.load.message.MessageHandler.Context;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestPrimaryToReplicaResourceFunction {

  private PrimaryToReplicaResourceFunction function;
  @Mock
  private HiveConf hiveConf;
  @Mock

  private Function functionObj;
  @Mock
  private FileSystem mockFs;

  MockedStatic<Time> timeMockedStatic;
  private static Logger logger =
      LoggerFactory.getLogger(TestPrimaryToReplicaResourceFunction.class);

  @Before
  public void setup() {
    MetaData metadata = new MetaData(null, null, null, null, functionObj);
    Context context =
        new Context("primaryDb", null, null, null, hiveConf, null, null, logger);
    when(hiveConf.getVar(HiveConf.ConfVars.REPL_FUNCTIONS_ROOT_DIR))
        .thenReturn("/someBasePath/withADir/");
    timeMockedStatic = mockStatic(Time.class);
    timeMockedStatic.when(Time::monotonicNowNanos).thenReturn(0L);
    function = new PrimaryToReplicaResourceFunction(context, metadata, "replicaDbName");
  }

  @After
  public void tearDown() {
    timeMockedStatic.close();
  }

  @Test
  public void createDestinationPath() throws IOException, SemanticException, URISyntaxException {
    MockedStatic<FileSystem> fileSystemMockedStatic = mockStatic(FileSystem.class);
    MockedStatic<ReplCopyTask> ignoredReplCopyTaskMockedStatic = mockStatic(ReplCopyTask.class);
    MockedStatic<CreateFunctionHandler> createFunctionHandlerMockedStatic = mockStatic(CreateFunctionHandler.class);

    fileSystemMockedStatic.when(() -> FileSystem.get(any(Configuration.class))).thenReturn(mockFs);
    fileSystemMockedStatic.when(() -> FileSystem.get(any(URI.class), any(Configuration.class))).thenReturn(mockFs);

    when(mockFs.getScheme()).thenReturn("hdfs");
    when(mockFs.getUri()).thenReturn(new URI("hdfs", "somehost:9000", null, null, null));

    when(functionObj.getFunctionName()).thenReturn("someFunctionName");
    Task mock = mock(Task.class);
    when(ReplCopyTask.getLoadCopyTask(any(ReplicationSpec.class), any(Path.class), any(Path.class),
        any(HiveConf.class), any(), any())).thenReturn(mock);

    ResourceUri resourceUri = function.destinationResourceUri(new ResourceUri(ResourceType.JAR,
        "hdfs://localhost:9000/user/someplace/ab.jar#e094828883"));

    assertThat(resourceUri.getUri(),
        is(equalTo(
            "hdfs://somehost:9000/someBasePath/withADir/replicadbname/somefunctionname/" + String
                .valueOf(0L) + "/ab.jar")));
  }
}