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
package org.apache.hadoop.hive.ql.exec.repl;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasReplInfo;
import org.apache.hadoop.hive.ql.parse.repl.ReplState;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

/**
 * Unit test class for testing Atlas metadata load.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestAtlasLoadTask {

  private AtlasLoadTask atlasLoadTask;

  @Mock
  private HiveConf conf;

  @Mock
  private AtlasLoadWork work;

  @Mock
  private ReplicationMetricCollector metricCollector;

  @Test
  public void testAtlasLoadMetrics() throws Exception {
    Mockito.when(work.getMetricCollector()).thenReturn(metricCollector);
    atlasLoadTask = new AtlasLoadTask(conf, work);
    AtlasLoadTask atlasLoadTaskSpy = Mockito.spy(atlasLoadTask);
    Mockito.when(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL)).thenReturn(true);
    Logger logger = Mockito.mock(Logger.class);
    Whitebox.setInternalState(ReplState.class, logger);
    AtlasReplInfo atlasReplInfo = new AtlasReplInfo("http://localhost:21000/atlas", "srcDB",
        "tgtDB", "srcCluster", "tgtCluster", new Path("hdfs://tmp"), null, conf);
    atlasReplInfo.setSrcFsUri("hdfs://srcFsUri:8020");
    atlasReplInfo.setTgtFsUri("hdfs:tgtFsUri:8020");
    Mockito.doReturn(atlasReplInfo).when(atlasLoadTaskSpy).createAtlasReplInfo();
    int status = atlasLoadTaskSpy.execute();
    Assert.assertEquals(0, status);
    ArgumentCaptor<String> replStateCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Object> eventDetailsCaptor = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(logger,
            Mockito.times(2)).info(replStateCaptor.capture(),
            eventCaptor.capture(), eventDetailsCaptor.capture());
    Assert.assertEquals("REPL::{}: {}", replStateCaptor.getAllValues().get(0));
    Assert.assertEquals("ATLAS_LOAD_START", eventCaptor.getAllValues().get(0));
    Assert.assertEquals("ATLAS_LOAD_END", eventCaptor.getAllValues().get(1));
    Assert.assertTrue(eventDetailsCaptor.getAllValues().get(0)
            .toString().contains("{\"sourceDbName\":\"srcDB\",\"targetDbName\":\"tgtDB\",\"loadStartTime\":"));
    Assert.assertTrue(eventDetailsCaptor
            .getAllValues().get(1).toString().contains("{\"sourceDbName\":\"srcDB\",\"targetDbName\""
                    + ":\"tgtDB\",\"numOfEntities\":0,\"loadEndTime\""));
  }
}
