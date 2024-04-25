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

import com.sun.jersey.api.client.ClientResponse;
import org.apache.atlas.AtlasBaseClient;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.impexp.AtlasExportRequest;
import org.apache.atlas.model.impexp.AtlasServer;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasReplInfo;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRequestBuilder;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRestClient;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRestClientBuilder;
import org.apache.hadoop.hive.ql.exec.repl.atlas.AtlasRestClientImpl;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.logging.log4j.Level;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test class for testing Atlas metadata Dump.
 */
@RunWith(MockitoJUnitRunner.class)
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
    Logger logger = LoggerFactory.getLogger("ReplState");
    StringAppender appender = StringAppender.createStringAppender(null);
    appender.addToLogger(logger.getName(), Level.INFO);
    appender.start();
    Mockito.doReturn(0L).when(atlasDumpTaskSpy)
            .dumpAtlasMetaData(Mockito.any(AtlasRequestBuilder.class), Mockito.any(AtlasReplInfo.class));
    Mockito.doNothing().when(atlasDumpTaskSpy).createDumpMetadata(Mockito.any(AtlasReplInfo.class),
            Mockito.any(Long.class));
    int status = atlasDumpTaskSpy.execute();
    Assert.assertEquals(0, status);
    String logStr = appender.getOutput();
    Assert.assertEquals(2, StringUtils.countMatches(logStr, "REPL::"));
    Assert.assertTrue(logStr.contains("ATLAS_DUMP_START"));
    Assert.assertTrue(logStr.contains("ATLAS_DUMP_END"));
    Assert.assertTrue(logStr.contains("{\"dbName\":\"srcDB\",\"dumpStartTime"));
    Assert.assertTrue(logStr.contains("{\"dbName\":\"srcDB\",\"dumpEndTime\""));
    appender.removeFromLogger(logger.getName());
  }

  @Test
  public void testAtlasRestClientBuilder() throws SemanticException {
    try (MockedStatic<UserGroupInformation> userGroupInformationMockedStatic = mockStatic(UserGroupInformation.class)) {
      userGroupInformationMockedStatic.when(UserGroupInformation::getLoginUser).thenReturn(mock(UserGroupInformation.class));

      AtlasRestClientBuilder atlasRestClientBuilder = new AtlasRestClientBuilder("http://localhost:31000");
      AtlasRestClient atlasClient = atlasRestClientBuilder.getClient(conf);
      Assert.assertNotNull(atlasClient);
    }
  }

  @Test
  public void testRetryingClientTimeBased() throws SemanticException, IOException, AtlasServiceException {
    AtlasClientV2 atlasClientV2 = mock(AtlasClientV2.class);
    AtlasExportRequest exportRequest = mock(AtlasExportRequest.class);
    String exportResponseData = "dumpExportContent";
    InputStream exportedMetadataIS = new ByteArrayInputStream(exportResponseData.getBytes(StandardCharsets.UTF_8));
    when(atlasClientV2.exportData(any(AtlasExportRequest.class))).thenReturn(exportedMetadataIS);
    when(exportRequest.toString()).thenReturn("dummyExportRequest");
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION, TimeUnit.SECONDS)).thenReturn(60L);
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY, TimeUnit.SECONDS)).thenReturn(1L);
    AtlasRestClient atlasClient = new AtlasRestClientImpl(atlasClientV2, conf);
    AtlasRestClientImpl atlasRestClientImpl = (AtlasRestClientImpl)atlasClient;
    InputStream inputStream = atlasRestClientImpl.exportData(exportRequest);
    ArgumentCaptor<AtlasExportRequest> expReqCaptor = ArgumentCaptor.forClass(AtlasExportRequest.class);
    verify(atlasClientV2, times(1)).exportData(expReqCaptor.capture());
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
    when(atlasClientV2.exportData(any(AtlasExportRequest.class))).thenThrow(atlasServiceException);
    when(exportRequest.toString()).thenReturn("dummyExportRequest");
    setupConfForRetry();
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
    verify(atlasClientV2, times(3)).exportData(expReqCaptor.capture());
    for (AtlasExportRequest atlasExportRequest: expReqCaptor.getAllValues()) {
      Assert.assertEquals(atlasExportRequest.toString(), "dummyExportRequest");
    }
    Assert.assertTrue(inputStream == null);
  }

  @Test
  public void testAtlasServerEntity() throws AtlasServiceException, SemanticException {
    AtlasClientV2 atlasClientV2 = mock(AtlasClientV2.class);
    AtlasServer atlasServer = mock(AtlasServer.class);
    when(atlasClientV2.getServer(Mockito.anyString())).thenReturn(atlasServer);
    AtlasRestClient atlasClient = new AtlasRestClientImpl(atlasClientV2, conf);
    AtlasServer atlasServerRet = atlasClient.getServer("src", conf);
    Assert.assertTrue(atlasServer == atlasServerRet);
  }

  @Test
  public void testAtlasServerEntityNotFound() throws AtlasServiceException, SemanticException {
    setupConfForRetry();
    AtlasServiceException atlasServiceException = getAtlasServiceException(ClientResponse.Status.NOT_FOUND);
    AtlasClientV2 atlasClientV2 = mock(AtlasClientV2.class);
    when(atlasClientV2.getServer(Mockito.anyString())).thenThrow(atlasServiceException);
    AtlasRestClient atlasClient = new AtlasRestClientImpl(atlasClientV2, conf);
    AtlasServer atlasServerRet = atlasClient.getServer("src", conf);
    Assert.assertNull(atlasServerRet);
    ArgumentCaptor<String> getServerReqCaptor = ArgumentCaptor.forClass(String.class);
    verify(atlasClientV2, times(1)).getServer(getServerReqCaptor.capture());
  }

  @Test
  public void testAtlasServerEntityRetryExhausted() throws AtlasServiceException {
    setupConfForRetry();
    AtlasServiceException atlasServiceException = getAtlasServiceException(ClientResponse.Status.BAD_REQUEST);
    AtlasClientV2 atlasClientV2 = mock(AtlasClientV2.class);
    when(atlasClientV2.getServer(Mockito.anyString())).thenThrow(atlasServiceException);
    AtlasRestClient atlasClient = new AtlasRestClientImpl(atlasClientV2, conf);
    try {
      atlasClient.getServer("src", conf);
      Assert.fail("Should have thrown SemanticException.");
    } catch (SemanticException ex) {
      Assert.assertTrue(ex.getMessage().contains("Retry exhausted for retryable error code"));
      Assert.assertTrue(atlasServiceException == ex.getCause());
    }
    ArgumentCaptor<String> getServerReqCaptor = ArgumentCaptor.forClass(String.class);
    verify(atlasClientV2, times(4)).getServer(getServerReqCaptor.capture());
  }

  @Test
  public void testAtlasClientTimeouts() throws Exception {
    try (MockedStatic<UserGroupInformation> userGroupInformationMockedStatic = mockStatic(UserGroupInformation.class);
         MockedStatic<ConfigurationConverter> configurationConverterMockedStatic = mockStatic(ConfigurationConverter.class)) {
      userGroupInformationMockedStatic.when(UserGroupInformation::getLoginUser).
              thenReturn(mock(UserGroupInformation.class));
      configurationConverterMockedStatic.when(() -> ConfigurationConverter.getConfiguration(any(Properties.class))).
              thenCallRealMethod();

      when(conf.getTimeVar(HiveConf.ConfVars.REPL_EXTERNAL_CLIENT_CONNECT_TIMEOUT,
              TimeUnit.MILLISECONDS)).thenReturn(20L);
      when(conf.getTimeVar(HiveConf.ConfVars.REPL_ATLAS_CLIENT_READ_TIMEOUT, TimeUnit.MILLISECONDS)).thenReturn(500L);
      AtlasRestClientBuilder atlasRestCleintBuilder = new AtlasRestClientBuilder("http://localhost:31000");
      AtlasRestClient atlasClient = atlasRestCleintBuilder.getClient(conf);
      Assert.assertNotNull(atlasClient);
      ArgumentCaptor<Properties> propsCaptor = ArgumentCaptor.forClass(Properties.class);
      configurationConverterMockedStatic.verify(() -> ConfigurationConverter.getConfiguration(propsCaptor.capture()));
      Assert.assertEquals("20", propsCaptor.getValue().getProperty(
              AtlasRestClientBuilder.ATLAS_PROPERTY_CONNECT_TIMEOUT_IN_MS));
      Assert.assertEquals("500", propsCaptor.getValue().getProperty(
              AtlasRestClientBuilder.ATLAS_PROPERTY_READ_TIMEOUT_IN_MS));
    }
  }

  @Test
  public void testCreateExportRequest() throws Exception {
    List<String> listOfTable = Arrays.asList(new String [] {"t1", "t2"});
    AtlasRequestBuilder atlasRequestBuilder = Mockito.spy(AtlasRequestBuilder.class);
    Mockito.doReturn(listOfTable).when(atlasRequestBuilder)
            .getFileAsList(any(Path.class), any(HiveConf.class));
    AtlasReplInfo atlasReplInfo = new AtlasReplInfo("http://localhost:31000", "srcDb", "tgtDb",
            "src","tgt", new Path("/tmp/staging"), new Path("/tmp/list"), conf);
    AtlasExportRequest atlasExportRequest = atlasRequestBuilder.createExportRequest(atlasReplInfo);
    List<AtlasObjectId> itemsToExport = atlasExportRequest.getItemsToExport();
    Assert.assertEquals(2, itemsToExport.size());
    Assert.assertEquals(AtlasRequestBuilder.ATLAS_TYPE_HIVE_TABLE, itemsToExport.get(0).getTypeName());
    Assert.assertEquals("srcdb.t1@src", itemsToExport.get(0).getUniqueAttributes().get(
            AtlasRequestBuilder.ATTRIBUTE_QUALIFIED_NAME));
    Assert.assertEquals(AtlasRequestBuilder.ATLAS_TYPE_HIVE_TABLE, itemsToExport.get(1).getTypeName());
    Assert.assertEquals("srcdb.t2@src", itemsToExport.get(1).getUniqueAttributes().get(
            AtlasRequestBuilder.ATTRIBUTE_QUALIFIED_NAME));
  }

  @Test
  public void testGetFileAsListRetry() throws Exception {
    AtlasRequestBuilder atlasRequestBuilder = Mockito.spy(AtlasRequestBuilder.class);
    FileSystem fs = Mockito.mock(FileSystem.class);
    Mockito.doReturn(fs).when(atlasRequestBuilder).getFileSystem(any(Path.class), any(HiveConf.class));
    when(fs.getFileStatus(any(Path.class))).thenThrow(new IOException("Unable to connect"));
    Path tableListPath = new Path("/tmp/list");
    AtlasReplInfo atlasReplInfo = new AtlasReplInfo("http://localhost:31000", "srcDb", "tgtDb",
            "src","tgt", new Path("/tmp/staging"), tableListPath, conf);
    setupConfForRetry();
    try {
      atlasRequestBuilder.createExportRequest(atlasReplInfo);
    } catch (Exception e) {
      Assert.assertEquals(SemanticException.class.getName(), e.getClass().getName());
      Assert.assertTrue(e.getMessage().contains("Unable to connect"));
    }
    ArgumentCaptor<Path> getServerReqCaptor = ArgumentCaptor.forClass(Path.class);
    verify(fs, times(4)).getFileStatus(getServerReqCaptor.capture());
    List<Path>  pathList = getServerReqCaptor.getAllValues();
    for (Path path: pathList) {
      Assert.assertTrue(tableListPath.equals(path));
    }
  }

  private void setupConfForRetry() {
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_TOTAL_DURATION, TimeUnit.SECONDS)).thenReturn(60L);
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_INTIAL_DELAY, TimeUnit.SECONDS)).thenReturn(10L);
    when(conf.getTimeVar(HiveConf.ConfVars.REPL_RETRY_MAX_DELAY_BETWEEN_RETRIES, TimeUnit.SECONDS)).thenReturn(20L);
    when(conf.getFloatVar(HiveConf.ConfVars.REPL_RETRY_BACKOFF_COEFFICIENT)).thenReturn(2.0f);
  }

  public AtlasServiceException getAtlasServiceException(ClientResponse.Status status) {
    AtlasBaseClient.API api = new AtlasBaseClient.API("/api/atlas/admin", HttpMethod.POST,
            Response.Status.fromStatusCode(status.getStatusCode()));
    ClientResponse response = Mockito.mock(ClientResponse.class);
    when(response.getStatus()).thenReturn(status.getStatusCode());
    AtlasServiceException atlasServiceException = new AtlasServiceException(api, response);
    return atlasServiceException;
  }
}
