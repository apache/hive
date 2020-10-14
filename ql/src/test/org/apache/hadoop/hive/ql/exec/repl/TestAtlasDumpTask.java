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

import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasReplInfo;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRequestBuilder;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRestClient;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRestClientBuilder;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRestClientImpl;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.ReplState;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Unit test class for testing Atlas metadata Dump.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class, UserGroupInformation.class})
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
      .dumpAtlasMetaData(Mockito.any(AtlasRequestBuilder.class), Mockito.any(AtlasReplInfo.class));
    Mockito.doNothing().when(atlasDumpTaskSpy).createDumpMetadata(Mockito.any(AtlasReplInfo.class),
      Mockito.any(Long.class));
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

  @Test
  public void testAtlasRestClientBuilder() throws SemanticException, IOException {
    mockStatic(UserGroupInformation.class);
    when(UserGroupInformation.getLoginUser()).thenReturn(mock(UserGroupInformation.class));
    AtlasRestClientBuilder atlasRestCleintBuilder = new AtlasRestClientBuilder("http://localhost:31000");
    AtlasRestClient atlasClient = atlasRestCleintBuilder.getClient(conf);
    Assert.assertTrue(atlasClient != null);
  }

  @Test
  public void testRetryingClientTimeBased() throws SemanticException, IOException, AtlasServiceException {
    AtlasClientV2 atlasClientV2 = mock(AtlasClientV2.class);
    AtlasExportRequest exportRequest = mock(AtlasExportRequest.class);
    String exportResponseData = "dumpExportContent";
    InputStream exportedMetadataIS = new ByteArrayInputStream(exportResponseData.getBytes(StandardCharsets.UTF_8));
    when(atlasClientV2.exportData(Mockito.any(AtlasExportRequest.class))).thenReturn(exportedMetadataIS);
    when(exportRequest.toString()).thenReturn("dummyExportRequest");
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION, TimeUnit.SECONDS)).thenReturn(60L);
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY, TimeUnit.SECONDS)).thenReturn(1L);
    AtlasRestClient atlasClient = new AtlasRestClientImpl(atlasClientV2, conf);
    AtlasRestClientImpl atlasRestClientImpl = (AtlasRestClientImpl)atlasClient;
    InputStream inputStream = atlasRestClientImpl.exportData(exportRequest);
    ArgumentCaptor<AtlasExportRequest> expReqCaptor = ArgumentCaptor.forClass(AtlasExportRequest.class);
    Mockito.verify(atlasClientV2, Mockito.times(1)).exportData(expReqCaptor.capture());
    Assert.assertEquals(expReqCaptor.getValue().toString(), "dummyExportRequest");
    byte[] exportResponseDataReadBytes = new byte[exportResponseData.length()];
    inputStream.read(exportResponseDataReadBytes);
    String exportResponseDataReadString = new String(exportResponseDataReadBytes, StandardCharsets.UTF_8);
    Assert.assertEquals(exportResponseData, exportResponseDataReadString);
  }

  @Test
  public void testRetryingClientTimeBasedExhausted() throws AtlasServiceException {
    AtlasClientV2 atlasClientV2 = mock(AtlasClientV2.class);
    AtlasExportRequest exportRequest = mock(AtlasExportRequest.class);
    AtlasServiceException atlasServiceException = mock(AtlasServiceException.class);
    when(atlasServiceException.getMessage()).thenReturn("import or export is in progress");
    when(atlasClientV2.exportData(Mockito.any(AtlasExportRequest.class))).thenThrow(atlasServiceException);
    when(exportRequest.toString()).thenReturn("dummyExportRequest");
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION, TimeUnit.SECONDS)).thenReturn(60L);
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY, TimeUnit.SECONDS)).thenReturn(10L);
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_MAX_DELAY_BETWEEN_RETRIES, TimeUnit.SECONDS)).thenReturn(20L);
    when(conf.getFloatVar(HiveConf.ConfVars.REPL_RETRY_BACKOFF_COEFFICIENT)).thenReturn(2.0f);
    AtlasRestClient atlasClient = new AtlasRestClientImpl(atlasClientV2, conf);
    AtlasRestClientImpl atlasRestClientImpl = (AtlasRestClientImpl)atlasClient;
    InputStream inputStream = null;
    try {
      inputStream = atlasRestClientImpl.exportData(exportRequest);
      Assert.fail("Should have thrown SemanticException.");
    } catch (SemanticException ex) {
      Assert.assertTrue(ex.getMessage().contains("Retry exhausted for retryable error code"));
      Assert.assertTrue(atlasServiceException == ex.getCause());
    }
    ArgumentCaptor<AtlasExportRequest> expReqCaptor = ArgumentCaptor.forClass(AtlasExportRequest.class);
    Mockito.verify(atlasClientV2, Mockito.times(3)).exportData(expReqCaptor.capture());
    for (AtlasExportRequest atlasExportRequest: expReqCaptor.getAllValues()) {
      Assert.assertEquals(atlasExportRequest.toString(), "dummyExportRequest");
    }
    Assert.assertTrue(inputStream == null);
  }
}
