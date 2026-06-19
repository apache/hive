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
package org.apache.hive.service.cli.session;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.QueryInfo;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskResult;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.tmpl.QueryProfileTmpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
    conf = new HiveConfForTest(getClass());
    conf.set("hive.support.concurrency", "false");

    sessionManager = new SessionManager(null, true);
    sessionManager.init(conf);
    sessionManager.start();
  }

  /**
   * Test if query display captures information on current/historic SQL operations.
   */
  @Test
  public void testQueryDisplay() throws Exception {
    HiveSession session = sessionManager
        .createSession(new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8, "testuser", "", "",
            new HashMap<String, String>(), false, "");
    session.getSessionState().setIsHiveServerQuery(false);
    SessionState.start(conf);
    OperationHandle opHandle1 = session.executeStatement("show databases", null);
    SessionState.start(conf);
    OperationHandle opHandle2 = session.executeStatement("show tables", null);

    List<QueryInfo> liveSqlOperations, historicSqlOperations;
    liveSqlOperations = sessionManager.getOperationManager().getLiveQueryInfos();
    historicSqlOperations = sessionManager.getOperationManager().getHistoricalQueryInfos();
    Assert.assertEquals(liveSqlOperations.size(), 2);
    Assert.assertEquals(historicSqlOperations.size(), 0);
    verifyDDL(liveSqlOperations.get(0), "show databases", opHandle1.getHandleIdentifier().toString(), false);
    verifyDDL(liveSqlOperations.get(1), "show tables", opHandle2.getHandleIdentifier().toString(), false);

    session.closeOperation(opHandle1);
    liveSqlOperations = sessionManager.getOperationManager().getLiveQueryInfos();
    historicSqlOperations = sessionManager.getOperationManager().getHistoricalQueryInfos();
    Assert.assertEquals(liveSqlOperations.size(), 1);
    Assert.assertEquals(historicSqlOperations.size(), 1);
    verifyDDL(historicSqlOperations.get(0), "show databases", opHandle1.getHandleIdentifier().toString(), true);
    verifyDDL(liveSqlOperations.get(0), "show tables", opHandle2.getHandleIdentifier().toString(), false);

    session.closeOperation(opHandle2);
    liveSqlOperations = sessionManager.getOperationManager().getLiveQueryInfos();
    historicSqlOperations = sessionManager.getOperationManager().getHistoricalQueryInfos();
    Assert.assertEquals(liveSqlOperations.size(), 0);
    Assert.assertEquals(historicSqlOperations.size(), 2);
    verifyDDL(historicSqlOperations.get(1), "show databases", opHandle1.getHandleIdentifier().toString(), true);
    verifyDDL(historicSqlOperations.get(0), "show tables", opHandle2.getHandleIdentifier().toString(), true);

    session.close();
  }

  /**
   * Test if webui captures information on current/historic SQL operations.
   */
  @Test
  public void testWebUI() throws Exception {
    HiveSession session = sessionManager
        .createSession(new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8, "testuser", "", "",
            new HashMap<String, String>(), false, "");

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

  /**
   * Test for the HiveConf option HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT.
   */
  @Test
  public void checkWebuiExplainOutput() throws Exception {

    //check cases when HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT is set and not set
    boolean[] webuiExplainConfValues = new boolean[]{true, false};

    for (boolean confValue : webuiExplainConfValues) {
      conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT, confValue);
      HiveSession session = sessionManager
          .createSession(new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),
              TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8, "testuser", "", "",
              new HashMap<String, String>(), false, "");
      SessionState.start(conf);

      OperationHandle opHandle = session.executeStatement("show tables", null);
      session.closeOperation(opHandle);
      //STAGE PLANS is something which will be shown as part of EXPLAIN query
      verifyDDLHtml("STAGE PLANS", opHandle.getHandleIdentifier().toString(), confValue);
      //Check that the following message is not shown when this option is set
      verifyDDLHtml(
          "Set configuration hive.server2.webui.explain.output to true to view future query plans",
          opHandle.getHandleIdentifier().toString(), !confValue);
      session.close();
    }
  }

  /**
   * Test for the HiveConf options HIVE_SERVER2_WEBUI_SHOW_GRAPH,
   * HIVE_SERVER2_WEBUI_MAX_GRAPH_SIZE.
   */
  @Test
  public void checkWebuiShowGraph() throws Exception {
    // WebUI-related boolean confs must be set before build, since the implementation of
    // QueryProfileTmpl.jamon depends on them.
    // They depend on HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT being set to true.
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_SHOW_GRAPH, true);

    HiveSession session = sessionManager
        .createSession(new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8, "testuser", "", "",
            new HashMap<String, String>(), false, "");
    SessionState.start(conf);

    session.getSessionConf()
        .setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_MAX_GRAPH_SIZE, 0);
    testGraphDDL(session, true);
    session.getSessionConf()
        .setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_MAX_GRAPH_SIZE, 40);
    testGraphDDL(session, false);

    session.close();
    resetConfToDefaults();
  }

  private void testGraphDDL(HiveSession session, boolean exceedMaxGraphSize) throws Exception {
    OperationHandle opHandleGraph = session.executeStatement("show tables", null);
    session.closeOperation(opHandleGraph);

    // Check for a query plan. If the graph size exceeds the max allowed, none should appear.
    verifyDDLHtml("Query information not available.",
        opHandleGraph.getHandleIdentifier().toString(), exceedMaxGraphSize);
    verifyDDLHtml("STAGE DEPENDENCIES",
        opHandleGraph.getHandleIdentifier().toString(), !exceedMaxGraphSize);
    // Check that if plan Json is there, it is not empty
    verifyDDLHtml("jsonPlan = {}", opHandleGraph.getHandleIdentifier().toString(), false);
  }

  /**
   * Test for the HiveConf option HIVE_SERVER2_WEBUI_SHOW_STATS, which is available for Tez
   * jobs only.
   */
  @Test
  public void checkWebUIShowStats() throws Exception {
    // WebUI-related boolean confs must be set before build. HIVE_SERVER2_WEBUI_SHOW_STATS depends
    // on HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT and HIVE_SERVER2_WEBUI_SHOW_GRAPH being set to true.
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_SHOW_GRAPH, true);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_MAX_GRAPH_SIZE, 40);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_SHOW_STATS, true);

    HiveSession session = sessionManager
        .createSession(new SessionHandle(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8),
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8, "testuser", "", "",
            new HashMap<String, String>(), false, "");
    SessionState.start(conf);

    OperationHandle opHandleSetup =
        session.executeStatement("CREATE TABLE statsTable (i int)", null);
    session.closeOperation(opHandleSetup);
    OperationHandle opHandleQuery =
        session.executeStatement("INSERT INTO statsTable VALUES (0)", null);
    session.closeOperation(opHandleQuery);

    // INSERT queries include a Tez task.
    verifyDDLHtml("DagId", opHandleQuery.getHandleIdentifier().toString(), true);

    session.close();
    resetConfToDefaults();
  }

  private void resetConfToDefaults() {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_EXPLAIN_OUTPUT, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_SHOW_GRAPH, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_SHOW_STATS, false);
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_MAX_GRAPH_SIZE, 25);
  }

  private void verifyDDL(QueryInfo queryInfo, String stmt, String handle, boolean finished) {

    Assert.assertEquals(queryInfo.getUserName(), "testuser");
    Assert.assertEquals(queryInfo.getExecutionEngine(), "tez");
    Assert.assertEquals(queryInfo.getOperationId(), handle);
    Assert.assertTrue(queryInfo.getBeginTime() > 0 && queryInfo.getBeginTime() <= System.currentTimeMillis());

    if (finished) {
      Assert.assertTrue(
          queryInfo.getEndTime() > 0 && queryInfo.getEndTime() >= queryInfo.getBeginTime()
              && queryInfo.getEndTime() <= System.currentTimeMillis());
      Assert.assertTrue(queryInfo.getRuntime() > 0);
    } else {
      Assert.assertNull(queryInfo.getEndTime());
      //For runtime, query may have finished.
    }

    QueryDisplay qDisplay1 = queryInfo.getQueryDisplay();
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

    Assert.assertTrue(qDisplay1.getPerfLogStarts(QueryDisplay.Phase.COMPILATION).containsKey(PerfLogger.COMPILE));
    Assert.assertFalse(qDisplay1.getPerfLogStarts(QueryDisplay.Phase.EXECUTION).containsKey(PerfLogger.COMPILE));
    Assert.assertTrue(qDisplay1.getPerfLogStarts(QueryDisplay.Phase.EXECUTION).containsKey(PerfLogger.DRIVER_EXECUTE));
    Assert.assertFalse(qDisplay1.getPerfLogStarts(QueryDisplay.Phase.COMPILATION)
        .containsKey(PerfLogger.DRIVER_EXECUTE));

    Assert.assertEquals(qDisplay1.getTaskDisplays().size(), 1);
    QueryDisplay.TaskDisplay tInfo1 = qDisplay1.getTaskDisplays().get(0);
    Assert.assertEquals(tInfo1.getTaskId(), "Stage-0");
    Assert.assertEquals(tInfo1.getTaskType(), StageType.DDL);
    Assert.assertTrue(tInfo1.getBeginTime() > 0 && tInfo1.getBeginTime() <= System.currentTimeMillis());
    Assert.assertTrue(tInfo1.getEndTime() > 0 && tInfo1.getEndTime() >= tInfo1.getBeginTime()
        && tInfo1.getEndTime() <= System.currentTimeMillis());
    Assert.assertEquals(tInfo1.getStatus(), "Success, ReturnVal 0");
  }

  /**
   * Sanity check if basic information is delivered in this html.  Let's not go too crazy and
   * assert each element, to make it easier to add UI improvements.
   */
  private void verifyDDLHtml(String stmt, String opHandle) throws Exception {
    verifyDDLHtml(stmt, opHandle, true);
  }

  private void verifyDDLHtml(String stmt, String opHandle, boolean assertCondition) throws Exception {
    StringWriter sw = new StringWriter();
    QueryInfo queryInfo = sessionManager.getOperationManager().getQueryInfo(opHandle);
    HiveConf hiveConf = sessionManager.getOperationManager().getHiveConf();
    new QueryProfileTmpl().render(sw, queryInfo, hiveConf);
    String html = sw.toString();
    Assert.assertEquals("HTML assert condition hasn't met, statement: " + stmt + ", html: " + html, assertCondition,
        html.contains(stmt));

    Assert.assertTrue("HTML doesn't contain and expected string, got: " + html, html.contains("testuser"));
  }

  static class MyTask extends Task<Integer> {

    public MyTask() {
      id = "x";
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    @Override
    public String getName() {
      return "my";

    }

    @Override
    public int execute() {
      return 0;
    }

    @Override
    public StageType getType() {
      return StageType.ATLAS_DUMP;

    }

  }

  @Test
  public void testJSONSerialization() throws Exception {
    QueryDisplay qd = new QueryDisplay();
    qd.setErrorMessage("asd");
    qd.setTaskResult("a", new TaskResult());
    qd.setExplainPlan("explainPlan");
    qd.setQueryStr("qstr");
    Task<?> tTask = new MyTask();
    qd.updateTaskStatus(tTask);
    tTask.setStarted();
    qd.updateTaskStatus(tTask);
    tTask.setDone();
    qd.updateTaskStatus(tTask);

    Long ee = qd.getTaskDisplays().get(0).getElapsedTime();
    System.out.println(ee);
    String json = QueryDisplay.OBJECT_MAPPER.writeValueAsString(qd);
    QueryDisplay n = QueryDisplay.OBJECT_MAPPER.readValue(json, QueryDisplay.class);

    assertEquals(qd.getQueryString(), n.getQueryString());
    assertEquals(qd.getExplainPlan(), n.getExplainPlan());
    assertEquals(qd.getErrorMessage(), n.getErrorMessage());
    assertEquals(qd.getTaskDisplays().size(), n.getTaskDisplays().size());
    assertEquals(qd.getTaskDisplays().get(0).taskState, n.getTaskDisplays().get(0).taskState);

  }

}
