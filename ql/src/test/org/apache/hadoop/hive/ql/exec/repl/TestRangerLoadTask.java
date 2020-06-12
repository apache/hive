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
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT;

/**
 * Unit test class for testing Ranger Dump.
 */
public class TestRangerLoadTask {

  protected static final Logger LOG = LoggerFactory.getLogger(TestRangerLoadTask.class);
  private RangerLoadTask task;

  @Mock
  private RangerRestClientImpl mockClient;

  @Mock
  private HiveConf conf;

  @Mock
  private RangerLoadWork work;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);
    task = new RangerLoadTask(mockClient, conf, work);
    Mockito.when(mockClient.changeDataSet(Mockito.anyList(), Mockito.anyString(), Mockito.anyString()))
      .thenCallRealMethod();
    Mockito.when(mockClient.checkConnection(Mockito.anyString())).thenReturn(true);
  }

  @Test
  public void testFailureInvalidAuthProviderEndpoint() {
    Mockito.when(conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT)).thenReturn(null);
    int status = task.execute();
    Assert.assertEquals(40000, status);
  }

  @Test
  public void testSuccessValidAuthProviderEndpoint() {
    Mockito.when(conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT)).thenReturn("rangerEndpoint");
    Mockito.when(work.getSourceDbName()).thenReturn("srcdb");
    Mockito.when(work.getTargetDbName()).thenReturn("tgtdb");
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
    Mockito.when(conf.getVar(REPL_AUTHORIZATION_PROVIDER_SERVICE_ENDPOINT)).thenReturn("rangerEndpoint");
    Mockito.when(work.getSourceDbName()).thenReturn("srcdb");
    Mockito.when(work.getTargetDbName()).thenReturn("tgtdb");
    Path rangerDumpPath = new Path("/tmp");
    Mockito.when(work.getCurrentDumpPath()).thenReturn(rangerDumpPath);
    mockClient.saveRangerPoliciesToFile(rangerPolicyList,
        rangerDumpPath, ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME, new HiveConf());
    Mockito.when(mockClient.readRangerPoliciesFromJsonFile(Mockito.any(), Mockito.any())).thenReturn(rangerPolicyList);
    int status = task.execute();
    Assert.assertEquals(0, status);
  }
}
