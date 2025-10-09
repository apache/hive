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
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerRestClientImpl;
import org.apache.hadoop.hive.ql.exec.repl.ranger.RangerPolicy;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.metadata.StringAppender;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;
import org.apache.logging.log4j.Level;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;

import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.RANGER_REST_URL;
import static org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils.RANGER_HIVE_SERVICE_NAME;
import static org.mockito.Mockito.when;

/**
 * Unit test class for testing Ranger Dump.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestRangerDumpTask {

  private RangerDumpTask task;

  @Mock
  private RangerRestClientImpl mockClient;

  @Mock
  private HiveConf conf;

  @Mock
  private RangerDumpWork work;

  @Mock
  private ReplicationMetricCollector metricCollector;

  @Before
  public void setup() throws Exception {
    task = new RangerDumpTask(mockClient, conf, work);
    when(mockClient.removeMultiResourcePolicies(Mockito.anyList())).thenCallRealMethod();
    when(mockClient.checkConnection(Mockito.anyString(), Mockito.any())).thenReturn(true);
    when(work.getMetricCollector()).thenReturn(metricCollector);
  }

  @Test
  public void testFailureInvalidAuthProviderEndpoint() throws Exception {
    when(conf.get(RANGER_REST_URL)).thenReturn(null);
    when(work.getDbName()).thenReturn("testdb");
    when(work.getCurrentDumpPath()).thenReturn(new Path("/tmp"));
    when(work.getRangerConfigResource()).thenReturn(new URL("file://ranger.xml"));
    int status = task.execute();
    Assert.assertEquals(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.getErrorCode(), status);
  }

  @Test
  public void testFailureInvalidRangerConfig() throws Exception {
    when(work.getDbName()).thenReturn("testdb");
    when(work.getCurrentDumpPath()).thenReturn(new Path("/tmp"));
    int status = task.execute();
    Assert.assertEquals(ErrorMsg.REPL_INVALID_CONFIG_FOR_SERVICE.getErrorCode(), status);
  }

  @Test
  public void testSuccessValidAuthProviderEndpoint() throws Exception {
    RangerExportPolicyList rangerPolicyList = new RangerExportPolicyList();
    rangerPolicyList.setPolicies(new ArrayList<RangerPolicy>());
    when(mockClient.exportRangerPolicies(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
      Mockito.any()))
      .thenReturn(rangerPolicyList);
    when(conf.get(RANGER_REST_URL)).thenReturn("rangerEndpoint");
    when(conf.get(RANGER_HIVE_SERVICE_NAME)).thenReturn("hive");
    when(work.getDbName()).thenReturn("testdb");
    when(work.getCurrentDumpPath()).thenReturn(new Path("/tmp"));
    when(work.getRangerConfigResource()).thenReturn(new URL("file://ranger.xml"));
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
    when(mockClient.exportRangerPolicies(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
      Mockito.any()))
      .thenReturn(rangerPolicyList);
    when(conf.get(RANGER_REST_URL)).thenReturn("rangerEndpoint");
    when(conf.get(RANGER_HIVE_SERVICE_NAME)).thenReturn("hive");
    when(work.getDbName()).thenReturn("testdb");
    Path rangerDumpPath = new Path("/tmp");
    when(work.getCurrentDumpPath()).thenReturn(rangerDumpPath);
    Path policyFile = new Path(rangerDumpPath, ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME);
    when(mockClient.saveRangerPoliciesToFile(rangerPolicyList, rangerDumpPath,
      ReplUtils.HIVE_RANGER_POLICIES_FILE_NAME, conf)).thenReturn(policyFile);
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
    RangerExportPolicyList rangerPolicyList = new RangerExportPolicyList();
    rangerPolicyList.setPolicies(new ArrayList<RangerPolicy>());
    Mockito.when(mockClient.exportRangerPolicies(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
      Mockito.any()))
      .thenReturn(rangerPolicyList);
    Mockito.when(conf.get(RANGER_REST_URL)).thenReturn("rangerEndpoint");
    Mockito.when(conf.get(RANGER_HIVE_SERVICE_NAME)).thenReturn("hive");
    Mockito.when(work.getDbName()).thenReturn("testdb");
    Mockito.when(work.getCurrentDumpPath()).thenReturn(new Path("/tmp"));
    Mockito.when(work.getRangerConfigResource()).thenReturn(new URL("file://ranger.xml"));
    int status = task.execute();
    Assert.assertEquals(0, status);
    String logStr = appender.getOutput();
    Assert.assertEquals(2, StringUtils.countMatches(logStr, "REPL::"));
    Assert.assertTrue(logStr.contains("RANGER_DUMP_START"));
    Assert.assertTrue(logStr.contains("RANGER_DUMP_END"));
    Assert.assertTrue(logStr.contains("{\"dbName\":\"testdb\",\"dumpStartTime"));
    Assert.assertTrue(logStr.contains("{\"dbName\":\"testdb\",\"actualNumPolicies\":0,\"dumpEndTime\""));
  }
}
