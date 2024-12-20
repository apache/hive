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
package org.apache.hive.service.cli.operation;

import com.google.common.collect.Maps;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.HandleIdentifier;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * TestSQLOperationMetrics
 */
public class TestSQLOperationMetrics {

  private SQLOperation operation;
  private CodahaleMetrics metrics;

  @Before
  public void setup() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    MetricsFactory.init(conf);

    HiveSession session = mock(HiveSession.class);
    when(session.getHiveConf()).thenReturn(conf);
    when(session.getSessionState()).thenReturn(mock(SessionState.class));
    when(session.getUserName()).thenReturn("userName");
    SessionHandle sessionHandle = mock(SessionHandle.class);
    when(sessionHandle.getHandleIdentifier()).thenReturn(new HandleIdentifier());
    when(session.getSessionHandle()).thenReturn(sessionHandle);

    operation = new SQLOperation(session, "select * from dummy",
        Maps.<String, String>newHashMap(), false, 0L);

    metrics = (CodahaleMetrics) MetricsFactory.getInstance();
  }

  @After
  public void tearDown() throws Exception {
    MetricsFactory.getInstance().close();
  }

  @Test
  public void testSubmittedQueryCount() throws Exception {
    String json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.TIMER,
        MetricsConstant.HS2_SUBMITTED_QURIES, "0");

    operation.onNewState(OperationState.FINISHED, OperationState.RUNNING);

    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.TIMER,
        MetricsConstant.HS2_SUBMITTED_QURIES, "1");
  }

  @Test
  public void testActiveUserQueriesCount() throws Exception {
    String name = MetricsConstant.SQL_OPERATION_PREFIX + "active_user";
    String json = ((CodahaleMetrics) metrics).dumpJson();

    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, name, "");

    operation.onNewState(OperationState.RUNNING, OperationState.INITIALIZED);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, name, "1");

    operation.onNewState(OperationState.RUNNING, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, name, "1");

    operation.onNewState(OperationState.FINISHED, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.COUNTER, name, "0");
  }

  @Test
  public void testSucceededQueriesCount() throws Exception {
    String json = ((CodahaleMetrics) metrics).dumpJson();

    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_SUCCEEDED_QUERIES, "");

    operation.onNewState(OperationState.FINISHED, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_SUCCEEDED_QUERIES, "1");

    operation.onNewState(OperationState.ERROR, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_SUCCEEDED_QUERIES, "1");

    operation.onNewState(OperationState.CANCELED, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_SUCCEEDED_QUERIES, "1");

    operation.onNewState(OperationState.FINISHED, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_SUCCEEDED_QUERIES, "2");
  }

  @Test
  public void testFailedQueriesCount() throws Exception {
    String json = ((CodahaleMetrics) metrics).dumpJson();

    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_FAILED_QUERIES, "");

    operation.onNewState(OperationState.ERROR, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_FAILED_QUERIES, "1");

    operation.onNewState(OperationState.FINISHED, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_FAILED_QUERIES, "1");

    operation.onNewState(OperationState.CANCELED, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_FAILED_QUERIES, "1");

    operation.onNewState(OperationState.ERROR, OperationState.RUNNING);
    json = ((CodahaleMetrics) metrics).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.METER,
        MetricsConstant.HS2_FAILED_QUERIES, "2");
  }


}
