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
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRequestBuilder;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRestClient;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.ReplState;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.mockito.ArgumentMatchers.any;

/**
 * Unit test class for testing Atlas metadata Dump.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class})
public class TestAtlasDumpTask {

  @Mock
  private AtlasRestClient atlasRestClient;

  private AtlasDumpTask atlasDumpTask;

  @Mock
  private HiveConf conf;

  @Mock
  private AtlasDumpWork work;

  @Mock
  private ReplicationMetricCollector metricCollector;

  @Test
  public void testAtlasDumpMetrics() throws Exception {
    Mockito.when(work.getMetricCollector()).thenReturn(metricCollector);
    Mockito.when(conf.get(HiveConf.ConfVars.REPL_ATLAS_ENDPOINT.varname)).thenReturn("http://localhost:21000/atlas");
    Mockito.when(conf.get(HiveConf.ConfVars.REPL_ATLAS_REPLICATED_TO_DB.varname)).thenReturn("tgtDb");
    Mockito.when(conf.get(HiveConf.ConfVars.REPL_SOURCE_CLUSTER_NAME.varname)).thenReturn("srcCluster");
    Mockito.when(conf.get(HiveConf.ConfVars.REPL_TARGET_CLUSTER_NAME.varname)).thenReturn("tgtCluster");
    Mockito.when(conf.get(ReplUtils.DEFAULT_FS_CONFIG)).thenReturn("hdfs:tgtFsUri:8020");
    Mockito.when(work.getStagingDir()).thenReturn(new Path("hdfs://tmp:8020/staging"));
    Mockito.when(work.getSrcDB()).thenReturn("srcDB");
    Mockito.when(work.isBootstrap()).thenReturn(true);
    atlasDumpTask = new AtlasDumpTask(atlasRestClient, conf, work);
    AtlasDumpTask atlasDumpTaskSpy = Mockito.spy(atlasDumpTask);
    Mockito.when(conf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_REPL)).thenReturn(true);
    Logger logger = Mockito.mock(Logger.class);
    Whitebox.setInternalState(ReplState.class, logger);
    Mockito.doReturn(0L).when(atlasDumpTaskSpy)
      .dumpAtlasMetaData(any(AtlasRequestBuilder.class), any(AtlasReplInfo.class));
    Mockito.doNothing().when(atlasDumpTaskSpy).createDumpMetadata(any(AtlasReplInfo.class), any(Long.class));
    int status = atlasDumpTaskSpy.execute();
    Assert.assertEquals(0, status);
    ArgumentCaptor<String> replStateCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Object> eventDetailsCaptor = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(logger,
        Mockito.times(2)).info(replStateCaptor.capture(),
        eventCaptor.capture(), eventDetailsCaptor.capture());
    Assert.assertEquals("REPL::{}: {}", replStateCaptor.getAllValues().get(0));
    Assert.assertEquals("ATLAS_DUMP_START", eventCaptor.getAllValues().get(0));
    Assert.assertEquals("ATLAS_DUMP_END", eventCaptor.getAllValues().get(1));
    Assert.assertTrue(eventDetailsCaptor.getAllValues().get(1).toString(), eventDetailsCaptor.getAllValues().get(0)
        .toString().contains("{\"dbName\":\"srcDB\",\"dumpStartTime"));
    Assert.assertTrue(eventDetailsCaptor
        .getAllValues().get(1).toString().contains("{\"dbName\":\"srcDB\",\"dumpEndTime\""));
  }
}
