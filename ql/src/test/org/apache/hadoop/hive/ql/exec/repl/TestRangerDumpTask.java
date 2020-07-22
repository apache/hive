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

import com.google.gson.Gson;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerExportPolicyList;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClientImpl;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerPolicy;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.parse.repl.ReplState;
import org.junit.Assert;
import org.junit.Before;
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

import java.util.ArrayList;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_RANGER_SERVICE_NAME;

/**
 * Unit test class for testing Ranger Dump.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({LoggerFactory.class})
public class TestRangerDumpTask {

  private RangerDumpTask task;

  @Mock
  private RangerRestClientImpl mockClient;

  @Mock
  private HiveConf conf;

  @Mock
  private RangerDumpWork work;

  @Before
  public void setup() throws Exception {
    task = new RangerDumpTask(mockClient, conf, work);
    Mockito.when(mockClient.removeMultiResourcePolicies(Mockito.anyList())).thenCallRealMethod();
    Mockito.when(mockClient.checkConnection(Mockito.anyString())).thenReturn(true);
  }

  @Test
  public void testFailureInvalidAuthProviderEndpoint() throws Exception {
    Mockito.when(conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT)).thenReturn(null);
    int status = task.execute();
    Assert.assertEquals(40000, status);
  }

  @Test
  public void testSuccessValidAuthProviderEndpoint() throws Exception {
    RangerExportPolicyList rangerPolicyList = new RangerExportPolicyList();
    rangerPolicyList.setPolicies(new ArrayList<RangerPolicy>());
    Mockito.when(mockClient.exportRangerPolicies(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
      .thenReturn(rangerPolicyList);
    Mockito.when(conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT)).thenReturn("rangerEndpoint");
    Mockito.when(conf.getVar(REPL_RANGER_SERVICE_NAME)).thenReturn("hive");
    Mockito.when(work.getDbName()).thenReturn("testdb");
    Mockito.when(work.getCurrentDumpPath()).thenReturn(new Path("/tmp"));
    int status = task.execute();
    Assert.assertEquals(0, status);
  }

  @Test
  public void testSuccessNonEmptyRangerPolicies() throws Exception {
    String rangerResponse = "{\"metaDataInfo\":{\"Host name\":\"ranger.apache.org\","
        + "\"Exported by\":\"hive\",\"Export time\":\"May 5, 2020, 8:55:03 AM\",\"Ranger apache version\""
        + ":\"2.0.0.7.2.0.0-61\"},\"policies\":[{\"service\":\"cm_hive\",\"name\":\"db-level\",\"policyType\":0,"
        + "\"description\":\"\",\"isAuditEnabled\":true,\"resources\":{\"database\":{\"values\":[\"aa\"],"
        + "\"isExcludes\":false,\"isRecursive\":false},\"column\":{\"values\":[\"id\"],\"isExcludes\":false,"
        + "\"isRecursive\":false},\"table\":{\"values\":[\"*\"],\"isExcludes\":false,\"isRecursive\":false}},"
        + "\"policyItems\":[{\"accesses\":[{\"type\":\"select\",\"isAllowed\":true},{\"type\":\"update\","
        + "\"isAllowed\":true}],\"users\":[\"admin\"],\"groups\":[\"public\"],\"conditions\":[],"
        + "\"delegateAdmin\":false}],\"denyPolicyItems\":[],\"allowExceptions\":[],\"denyExceptions\":[],"
        + "\"dataMaskPolicyItems\":[],\"rowFilterPolicyItems\":[],\"id\":40,\"guid\":"
        + "\"4e2b3406-7b9a-4004-8cdf-7a239c8e2cae\",\"isEnabled\":true,\"version\":1}]}";
    RangerExportPolicyList rangerPolicyList = new Gson().fromJson(rangerResponse, RangerExportPolicyList.class);
    Mockito.when(mockClient.exportRangerPolicies(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
      .thenReturn(rangerPolicyList);
    Mockito.when(conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT)).thenReturn("rangerEndpoint");
    Mockito.when(conf.getVar(REPL_RANGER_SERVICE_NAME)).thenReturn("hive");
    Mockito.when(work.getDbName()).thenReturn("testdb");
    Path rangerDumpPath = new Path("/tmp");
    Mockito.when(work.getCurrentDumpPath()).thenReturn(rangerDumpPath);
    Path policyFile = new Path(rangerDumpPath, ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME);
    Mockito.when(mockClient.saveRangerPoliciesToFile(rangerPolicyList, rangerDumpPath,
      ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME, conf)).thenReturn(policyFile);
    int status = task.execute();
    Assert.assertEquals(0, status);
  }

  @Test
  public void testSuccessRangerDumpMetrics() throws Exception {
    Logger logger = Mockito.mock(Logger.class);
    Whitebox.setInternalState(ReplState.class, logger);
    RangerExportPolicyList rangerPolicyList = new RangerExportPolicyList();
    rangerPolicyList.setPolicies(new ArrayList<RangerPolicy>());
    Mockito.when(mockClient.exportRangerPolicies(Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
      .thenReturn(rangerPolicyList);
    Mockito.when(conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT)).thenReturn("rangerEndpoint");
    Mockito.when(conf.getVar(REPL_RANGER_SERVICE_NAME)).thenReturn("hive");
    Mockito.when(work.getDbName()).thenReturn("testdb");
    Mockito.when(work.getCurrentDumpPath()).thenReturn(new Path("/tmp"));
    int status = task.execute();
    Assert.assertEquals(0, status);
    ArgumentCaptor<String> replStateCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Object> eventCaptor = ArgumentCaptor.forClass(Object.class);
    ArgumentCaptor<Object> eventDetailsCaptor = ArgumentCaptor.forClass(Object.class);
    Mockito.verify(logger,
        Mockito.times(2)).info(replStateCaptor.capture(),
        eventCaptor.capture(), eventDetailsCaptor.capture());
    Assert.assertEquals("REPL::{}: {}", replStateCaptor.getAllValues().get(0));
    Assert.assertEquals("RANGER_DUMP_START", eventCaptor.getAllValues().get(0));
    Assert.assertEquals("RANGER_DUMP_END", eventCaptor.getAllValues().get(1));
    Assert.assertTrue(eventDetailsCaptor.getAllValues().get(0)
        .toString().contains("{\"dbName\":\"testdb\",\"dumpStartTime"));
    Assert.assertTrue(eventDetailsCaptor
        .getAllValues().get(1).toString().contains("{\"dbName\":\"testdb\",\"actualNumPolicies\":0,\"dumpEndTime\""));
  }
}
