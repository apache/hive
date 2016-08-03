/**
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
package org.apache.hive.service.cli.session;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.operation.SQLOperationDisplay;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.server.HiveServer2;
import org.apache.hive.tmpl.QueryProfileTmpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;

/**
 * Test QueryDisplay and its consumers like WebUI.
 */
public class TestQueryDisplay {
  private HiveConf conf;
  private SessionManager sessionManager;


  @Before
  public void setup() {
    conf = new HiveConf();
    conf.set("hive.support.concurrency", "false");

    HiveServer2 dummyHs2 = new HiveServer2();
    sessionManager = new SessionManager(dummyHs2);
    sessionManager.init(conf);
  }

  /**
   * Test if query display captures information on current/historic SQL operations.
   */
  @Test
  public void testQueryDisplay() throws Exception {
    HiveSession session = sessionManager.createSession(
      new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8,
      "testuser", "", "", new HashMap<String, String>(), false, "");

    SessionState.start(conf);
    OperationHandle opHandle1 = session.executeStatement("show databases", null);
    SessionState.start(conf);
    OperationHandle opHandle2 = session.executeStatement("show tables", null);


    List<SQLOperationDisplay> liveSqlOperations, historicSqlOperations;
    liveSqlOperations = sessionManager.getOperationManager().getLiveSqlOperations();
    historicSqlOperations = sessionManager.getOperationManager().getHistoricalSQLOperations();
    Assert.assertEquals(liveSqlOperations.size(), 2);
    Assert.assertEquals(historicSqlOperations.size(), 0);
    verifyDDL(liveSqlOperations.get(0), "show databases", opHandle1.getHandleIdentifier().toString(), false);
    verifyDDL(liveSqlOperations.get(1),"show tables", opHandle2.getHandleIdentifier().toString(), false);

    session.closeOperation(opHandle1);
    liveSqlOperations = sessionManager.getOperationManager().getLiveSqlOperations();
    historicSqlOperations = sessionManager.getOperationManager().getHistoricalSQLOperations();
    Assert.assertEquals(liveSqlOperations.size(), 1);
    Assert.assertEquals(historicSqlOperations.size(), 1);
    verifyDDL(historicSqlOperations.get(0),"show databases", opHandle1.getHandleIdentifier().toString(), true);
    verifyDDL(liveSqlOperations.get(0),"show tables", opHandle2.getHandleIdentifier().toString(), false);

    session.closeOperation(opHandle2);
    liveSqlOperations = sessionManager.getOperationManager().getLiveSqlOperations();
    historicSqlOperations = sessionManager.getOperationManager().getHistoricalSQLOperations();
    Assert.assertEquals(liveSqlOperations.size(), 0);
    Assert.assertEquals(historicSqlOperations.size(), 2);
    verifyDDL(historicSqlOperations.get(1),"show databases", opHandle1.getHandleIdentifier().toString(), true);
    verifyDDL(historicSqlOperations.get(0),"show tables", opHandle2.getHandleIdentifier().toString(), true);

    session.close();
  }

  /**
   * Test if webui captures information on current/historic SQL operations.
   */
  @Test
  public void testWebUI() throws Exception {
    HiveSession session = sessionManager.createSession(
      new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),
      TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8,
      "testuser", "", "", new HashMap<String, String>(), false, "");

    SessionState.start(conf);
    OperationHandle opHandle1 = session.executeStatement("show databases", null);
    SessionState.start(conf);
    OperationHandle opHandle2 = session.executeStatement("show tables", null);

    verifyDDLHtml("show databases", opHandle1.getHandleIdentifier().toString());
    verifyDDLHtml("show tables", opHandle2.getHandleIdentifier().toString());

    session.closeOperation(opHandle1);
    session.closeOperation(opHandle2);

    verifyDDLHtml("show databases", opHandle1.getHandleIdentifier().toString());
    verifyDDLHtml("show tables", opHandle2.getHandleIdentifier().toString());

    session.close();
  }

  private void verifyDDL(SQLOperationDisplay display, String stmt, String handle, boolean finished) {

    Assert.assertEquals(display.getUserName(), "testuser");
    Assert.assertEquals(display.getExecutionEngine(), "mr");
    Assert.assertEquals(display.getOperationId(), handle);
    Assert.assertTrue(display.getBeginTime() > 0 && display.getBeginTime() <= System.currentTimeMillis());

    if (finished) {
      Assert.assertTrue(display.getEndTime() > 0 && display.getEndTime() >= display.getBeginTime()
        && display.getEndTime() <= System.currentTimeMillis());
      Assert.assertTrue(display.getRuntime() > 0);
    } else {
      Assert.assertNull(display.getEndTime());
      //For runtime, query may have finished.
    }

    QueryDisplay qDisplay1 = display.getQueryDisplay();
    Assert.assertNotNull(qDisplay1);
    Assert.assertEquals(qDisplay1.getQueryString(), stmt);
    Assert.assertNotNull(qDisplay1.getExplainPlan());
    Assert.assertNull(qDisplay1.getErrorMessage());

    Assert.assertTrue(qDisplay1.getHmsTimings(QueryDisplay.Phase.COMPILATION).size() > 0);
    Assert.assertTrue(qDisplay1.getHmsTimings(QueryDisplay.Phase.EXECUTION).size() > 0);

    Assert.assertTrue(qDisplay1.getPerfLogStarts(QueryDisplay.Phase.COMPILATION).size() > 0);
    Assert.assertTrue(qDisplay1.getPerfLogEnds(QueryDisplay.Phase.COMPILATION).size() > 0);

    Assert.assertTrue(qDisplay1.getPerfLogStarts(QueryDisplay.Phase.COMPILATION).size() > 0);
    Assert.assertTrue(qDisplay1.getPerfLogEnds(QueryDisplay.Phase.COMPILATION).size() > 0);

    Assert.assertEquals(qDisplay1.getTaskDisplays().size(), 1);
    QueryDisplay.TaskDisplay tInfo1 = qDisplay1.getTaskDisplays().get(0);
    Assert.assertEquals(tInfo1.getTaskId(), "Stage-0");
    Assert.assertEquals(tInfo1.getTaskType(), StageType.DDL);
    Assert.assertTrue(tInfo1.getBeginTime() > 0 && tInfo1.getBeginTime() <= System.currentTimeMillis());
    Assert.assertTrue(tInfo1.getEndTime() > 0 && tInfo1.getEndTime() >= tInfo1.getBeginTime() &&
      tInfo1.getEndTime() <= System.currentTimeMillis());
    Assert.assertEquals(tInfo1.getStatus(), "Success, ReturnVal 0");
  }

  /**
   * Sanity check if basic information is delivered in this html.  Let's not go too crazy and
   * assert each element, to make it easier to add UI improvements.
   */
  private void verifyDDLHtml(String stmt, String opHandle) throws Exception {
    StringWriter sw = new StringWriter();
    SQLOperationDisplay sod = sessionManager.getOperationManager().getSQLOperationDisplay(
      opHandle);
    new QueryProfileTmpl().render(sw, sod);
    String html = sw.toString();

    Assert.assertTrue(html.contains(stmt));
    Assert.assertTrue(html.contains("testuser"));
  }

}
