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
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerExportPolicyList;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerPolicy;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClientImpl;
import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.logging.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.RANGER_HIVE_SERVICE_NAME;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.RANGER_REST_URL;
import static org.mockito.Mockito.when;

/**
 * Unit test class for testing Ranger Load.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestRangerLoadTask {

  protected static final Logger LOG = LoggerFactory.getLogger(TestRangerLoadTask.class);
  private RangerLoadTask task;

  @Mock
  private RangerRestClientImpl mockClient;

  @Mock
  private HiveConf conf;

  @Mock
  private RangerLoadWork work;

  @Mock
  private RangerDenyWork rangerDenyWork;

  @Mock
  private ReplicationMetricCollector metricCollector;

  @Before
  public void setup() throws Exception {
    task = new RangerLoadTask(mockClient, conf, work);
    when(mockClient.changeDataSet(Mockito.anyList(), Mockito.anyString(), Mockito.anyString()))
      .thenCallRealMethod();
    when(mockClient.getDenyPolicyForReplicatedDb(Mockito.anyString(), Mockito.anyString(),
      Mockito.anyString())).thenCallRealMethod();
    when(mockClient.checkConnection(Mockito.anyString(), Mockito.any())).thenReturn(true);
    when(work.getMetricCollector()).thenReturn(metricCollector);
  }

  @Test
  public void testFailureInvalidAuthProviderEndpoint() {
    when(work.getCurrentDumpPath()).thenReturn(new Path("dumppath"));
    int status = task.execute();
    Assert.assertEquals(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.getErrorCode(), status);
  }

  @Test
  public void testSuccessValidAuthProviderEndpoint() throws MalformedURLException {
    when(conf.get(RANGER_REST_URL)).thenReturn("rangerEndpoint");
    when(work.getSourceDbName()).thenReturn("srcdb");
    when(work.getTargetDbName()).thenReturn("tgtdb");
    when(work.getRangerConfigResource()).thenReturn(new URL("file://ranger.xml"));
    int status = task.execute();
    Assert.assertEquals(0, status);
  }

  @Test
  public void testSuccessNonEmptyRangerPolicies() throws Exception {
    String rangerResponse = "{\"metaDataInfo\":{\"Host name\":\"ranger.apache.org\","
        + "\"Exported by\":\"hive\",\"Export time\":\"May 5, 2020, 8:55:03 AM\",\"Ranger apache version\""
        + ":\"2.0.0.7.2.0.0-61\"},\"policies\":[{\"service\":\"hive\",\"name\":\"db-level\",\"policyType\":0,"
        + "\"description\":\"\",\"isAuditEnabled\":true,\"resources\":{\"database\":{\"values\":[\"aa\"],"
        + "\"isExcludes\":false,\"isRecursive\":false},\"column\":{\"values\":[\"id\"],\"isExcludes\":false,"
        + "\"isRecursive\":false},\"table\":{\"values\":[\"*\"],\"isExcludes\":false,\"isRecursive\":false}},"
        + "\"policyItems\":[{\"accesses\":[{\"type\":\"select\",\"isAllowed\":true},{\"type\":\"update\","
        + "\"isAllowed\":true}],\"users\":[\"admin\"],\"groups\":[\"public\"],\"conditions\":[],"
        + "\"delegateAdmin\":false}],\"denyPolicyItems\":[],\"allowExceptions\":[],\"denyExceptions\":[],"
        + "\"dataMaskPolicyItems\":[],\"rowFilterPolicyItems\":[],\"id\":40,\"guid\":"
        + "\"4e2b3406-7b9a-4004-8cdf-7a239c8e2cae\",\"isEnabled\":true,\"version\":1}]}";
    RangerExportPolicyList rangerPolicyList = new Gson().fromJson(rangerResponse, RangerExportPolicyList.class);
    when(conf.get(RANGER_REST_URL)).thenReturn("rangerEndpoint");
    when(work.getSourceDbName()).thenReturn("srcdb");
    when(work.getTargetDbName()).thenReturn("tgtdb");
    Path rangerDumpPath = new Path("/tmp");
    when(work.getCurrentDumpPath()).thenReturn(rangerDumpPath);
    when(mockClient.readRangerPoliciesFromJsonFile(Mockito.any(), Mockito.any())).thenReturn(rangerPolicyList);
    when(work.getRangerConfigResource()).thenReturn(new URL("file://ranger.xml"));
    int status = task.execute();
    Assert.assertEquals(0, status);
  }

  @Test
  public void testSuccessRangerDumpMetrics() throws Exception {
    Logger logger = LoggerFactory.getLogger("ReplState");
    StringAppender appender = StringAppender.createStringAppender(null);
    appender.addToLogger(logger.getName(), Level.INFO);
    appender.start();
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
    Mockito.when(conf.get(RANGER_REST_URL)).thenReturn("rangerEndpoint");
    Mockito.when(work.getSourceDbName()).thenReturn("srcdb");
    Mockito.when(work.getTargetDbName()).thenReturn("tgtdb");
    Path rangerDumpPath = new Path("/tmp");
    Mockito.when(work.getCurrentDumpPath()).thenReturn(rangerDumpPath);
    Mockito.when(mockClient.readRangerPoliciesFromJsonFile(Mockito.any(), Mockito.any())).thenReturn(rangerPolicyList);
    Mockito.when(work.getRangerConfigResource()).thenReturn(new URL("file://ranger.xml"));
    int status = task.execute();
    Assert.assertEquals(0, status);
    String logStr = appender.getOutput();
    Assert.assertEquals(2, StringUtils.countMatches(logStr, "REPL::"));
    Assert.assertTrue(logStr.contains("RANGER_LOAD_START"));
    Assert.assertTrue(logStr.contains("RANGER_LOAD_END"));
    Assert.assertTrue(logStr.contains("{\"sourceDbName\":\"srcdb\",\"targetDbName\":\"tgtdb\""
            + ",\"estimatedNumPolicies\":1,\"loadStartTime\":"));
    Assert.assertTrue(logStr.contains("{\"sourceDbName\":\"srcdb\",\"targetDbName\""
            + ":\"tgtdb\",\"actualNumPolicies\":1,\"loadEndTime\""));
    appender.removeFromLogger(logger.getName());
  }

  @Test
  public void testRangerDenyTask() throws Exception {
    RangerDenyTask rangerDenyTask = new RangerDenyTask(mockClient, conf, rangerDenyWork);
    when(conf.get(RANGER_REST_URL)).thenReturn("rangerEndpoint");
    when(rangerDenyWork.getSourceDbName()).thenReturn("srcdb");
    when(rangerDenyWork.getTargetDbName()).thenReturn("tgtdb");
    when(rangerDenyWork.getRangerConfigResource()).thenReturn(new URL("file://ranger.xml"));
    when(rangerDenyWork.getMetricCollector()).thenReturn(metricCollector);
    when(conf.getBoolVar(HiveConf.ConfVars.REPL_RANGER_ADD_DENY_POLICY_TARGET)).thenReturn(true);
    when(conf.get(RANGER_HIVE_SERVICE_NAME)).thenReturn("hive");
    int status = rangerDenyTask.execute();
    Assert.assertEquals(0, status);
    ArgumentCaptor<RangerExportPolicyList> rangerPolicyCapture = ArgumentCaptor.forClass(RangerExportPolicyList.class);
    ArgumentCaptor<String> rangerEndpoint = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> serviceName = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> targetDb = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<HiveConf> confCaptor = ArgumentCaptor.forClass(HiveConf.class);
    ArgumentCaptor<String> policyName = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockClient,
            Mockito.never()).deleteRangerPolicy(policyName.capture(), rangerEndpoint.capture(), serviceName.capture(),
            confCaptor.capture());
    Mockito.verify(mockClient,
            Mockito.times(1)).importRangerPolicies(rangerPolicyCapture.capture(),
            targetDb.capture(), rangerEndpoint.capture(), serviceName.capture(), confCaptor.capture());
    Assert.assertEquals("tgtdb", targetDb.getAllValues().get(0));
    Assert.assertEquals("rangerEndpoint", rangerEndpoint.getAllValues().get(0));
    Assert.assertEquals("hive", serviceName.getAllValues().get(0));
    RangerExportPolicyList actualPolicyList = rangerPolicyCapture.getAllValues().get(0);
    //Deny policy is added
    Assert.assertEquals(1, actualPolicyList.getListSize());
    RangerPolicy denyPolicy = actualPolicyList.getPolicies().get(0);
    Assert.assertEquals("hive", denyPolicy.getService());
    Assert.assertEquals("srcdb_replication deny policy for tgtdb", denyPolicy.getName());
    Assert.assertEquals(1, denyPolicy.getDenyExceptions().size());
    Assert.assertEquals("public", denyPolicy.getDenyPolicyItems().get(0).getGroups().get(0));
    Assert.assertEquals(8, denyPolicy.getDenyPolicyItems().get(0).getAccesses().size());
    boolean isReplAdminDenied = false;
    for (RangerPolicy.RangerPolicyItemAccess access : denyPolicy.getDenyPolicyItems().get(0).getAccesses()) {
      if (access.getType().equalsIgnoreCase("ReplAdmin")) {
        isReplAdminDenied = true;
      }
    }
    Assert.assertTrue(isReplAdminDenied);
    //Deny exception is for hive user. Deny exception is not for repl admin permission
    Assert.assertEquals("hive", denyPolicy.getDenyExceptions().get(0).getUsers().get(0));
    Assert.assertEquals(10, denyPolicy.getDenyExceptions().get(0).getAccesses().size());
    isReplAdminDenied = false;
    for (RangerPolicy.RangerPolicyItemAccess access : denyPolicy.getDenyExceptions().get(0).getAccesses()) {
      if (access.getType().equalsIgnoreCase("ReplAdmin")) {
        isReplAdminDenied = true;
      }
    }
    Assert.assertTrue(isReplAdminDenied);
    when(conf.getBoolVar(HiveConf.ConfVars.REPL_RANGER_ADD_DENY_POLICY_TARGET)).thenReturn(false);
    status = rangerDenyTask.execute();
    Assert.assertEquals(0, status);
    Mockito.verify(mockClient,
            Mockito.times(1)).deleteRangerPolicy(policyName.capture(), rangerEndpoint.capture(), serviceName.capture(),
            confCaptor.capture());
    Assert.assertEquals("srcdb_replication deny policy for tgtdb", policyName.getAllValues().get(0));
    Assert.assertEquals("rangerEndpoint", rangerEndpoint.getAllValues().get(0));
    Assert.assertEquals("hive", serviceName.getAllValues().get(0));
  }

  @Test
  public void testRangerEndpointCreation() throws Exception {
    RangerRestClientImpl rangerRestClient = new RangerRestClientImpl();
    Assert.assertTrue(rangerRestClient.getRangerExportUrl("http://ranger.apache.org:6080",
      "hive", "dbname").equals("http://ranger.apache.org:6080/service/plugins/"
      + "policies/exportJson?serviceName=hive&polResource=dbname&resource%3Adatabase=dbname&serviceType=hive"
      + "&resourceMatchScope=self_or_ancestor&resourceMatch=full"));

    Assert.assertTrue(rangerRestClient.getRangerExportUrl("http://ranger.apache.org:6080/",
      "hive", "dbname").equals("http://ranger.apache.org:6080/service/plugins/"
      + "policies/exportJson?serviceName=hive&polResource=dbname&resource%3Adatabase=dbname&serviceType=hive"
      + "&resourceMatchScope=self_or_ancestor&resourceMatch=full"));

    Assert.assertTrue(rangerRestClient.getRangerImportUrl("http://ranger.apache.org:6080/",
      "dbname").equals("http://ranger.apache.org:6080/service/plugins/policies/importPoliciesFromFile"
      + "?updateIfExists=true&polResource=dbname&policyMatchingAlgorithm=matchByName"));

    Assert.assertTrue(rangerRestClient.getRangerImportUrl("http://ranger.apache.org:6080",
      "dbname").equals("http://ranger.apache.org:6080/service/plugins/policies/importPoliciesFromFile"
      + "?updateIfExists=true&polResource=dbname&policyMatchingAlgorithm=matchByName"));

  }

  @Test
  public void testChangeDataSet() throws Exception {
    RangerRestClientImpl rangerRestClient = new RangerRestClientImpl();
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
    List<RangerPolicy> rangerPolicies = rangerPolicyList.getPolicies();
    rangerRestClient.changeDataSet(rangerPolicies, null, null);
    assertEqualsRangerPolicies(rangerPolicies, rangerRestClient.changeDataSet(rangerPolicies,
      null, null), "aa");
    assertEqualsRangerPolicies(rangerPolicies, rangerRestClient.changeDataSet(rangerPolicies,
      "aa", null), "aa");
    assertEqualsRangerPolicies(rangerPolicies, rangerRestClient.changeDataSet(rangerPolicies,
      null, "aa"), "aa");
    assertEqualsRangerPolicies(rangerPolicies, rangerRestClient.changeDataSet(rangerPolicies,
      "aa", "aa"), "aa");
    assertNotEqualsRangerPolicies(rangerPolicies, rangerRestClient.changeDataSet(rangerPolicies,
      "aa", "tgt_aa"), "tgt_aa");
  }

  private void assertNotEqualsRangerPolicies(List<RangerPolicy> expectedRangerPolicies,
                                          List<RangerPolicy> actualRangerPolicies, String targetName) {
    Assert.assertEquals(expectedRangerPolicies.size(), actualRangerPolicies.size());
    for (int index = 0; index < expectedRangerPolicies.size(); index++) {
      Assert.assertEquals(expectedRangerPolicies.get(index).getName(), actualRangerPolicies.get(index).getName());
      Assert.assertEquals(expectedRangerPolicies.get(index).getService(), actualRangerPolicies.get(index).getService());
      Assert.assertEquals(expectedRangerPolicies.get(index).getDescription(),
        actualRangerPolicies.get(index).getDescription());
      Assert.assertEquals(expectedRangerPolicies.get(index).getPolicyType(),
        actualRangerPolicies.get(index).getPolicyType());
      Assert.assertEquals(expectedRangerPolicies.get(index).getResources().size(),
        actualRangerPolicies.get(index).getResources().size());
      Assert.assertEquals(expectedRangerPolicies.get(index).getResources().size(),
        actualRangerPolicies.get(index).getResources().size());
      RangerPolicy.RangerPolicyResource expectedRangerPolicyResource = expectedRangerPolicies.get(index)
        .getResources().get("database");
      RangerPolicy.RangerPolicyResource actualRangerPolicyResource = actualRangerPolicies.get(index)
        .getResources().get("database");
      Assert.assertEquals(expectedRangerPolicyResource.getValues().size(),
        actualRangerPolicyResource.getValues().size());
      for (int resourceIndex = 0; resourceIndex < expectedRangerPolicyResource.getValues().size(); resourceIndex++) {
        Assert.assertEquals(actualRangerPolicyResource.getValues().get(index),
          targetName);
      }
    }
  }

  private void assertEqualsRangerPolicies(List<RangerPolicy> expectedRangerPolicies,
                                          List<RangerPolicy> actualRangerPolicies, String sourceName) {
    Assert.assertEquals(expectedRangerPolicies.size(), actualRangerPolicies.size());
    for (int index = 0; index < expectedRangerPolicies.size(); index++) {
      Assert.assertEquals(expectedRangerPolicies.get(index).getName(), actualRangerPolicies.get(index).getName());
      Assert.assertEquals(expectedRangerPolicies.get(index).getService(), actualRangerPolicies.get(index).getService());
      Assert.assertEquals(expectedRangerPolicies.get(index).getDescription(),
        actualRangerPolicies.get(index).getDescription());
      Assert.assertEquals(expectedRangerPolicies.get(index).getPolicyType(),
        actualRangerPolicies.get(index).getPolicyType());
      Assert.assertEquals(expectedRangerPolicies.get(index).getResources().size(),
        actualRangerPolicies.get(index).getResources().size());
      Assert.assertEquals(expectedRangerPolicies.get(index).getResources().size(),
        actualRangerPolicies.get(index).getResources().size());
      RangerPolicy.RangerPolicyResource expectedRangerPolicyResource = expectedRangerPolicies.get(index)
        .getResources().get("database");
      RangerPolicy.RangerPolicyResource actualRangerPolicyResource = actualRangerPolicies.get(index)
        .getResources().get("database");
      Assert.assertEquals(expectedRangerPolicyResource.getValues().size(),
        actualRangerPolicyResource.getValues().size());
      for (int resourceIndex = 0; resourceIndex < expectedRangerPolicyResource.getValues().size(); resourceIndex++) {
        Assert.assertEquals(expectedRangerPolicyResource.getValues().get(index),
          actualRangerPolicyResource.getValues().get(index));
        Assert.assertEquals(actualRangerPolicyResource.getValues().get(index),
          sourceName);
      }
    }
  }
}
