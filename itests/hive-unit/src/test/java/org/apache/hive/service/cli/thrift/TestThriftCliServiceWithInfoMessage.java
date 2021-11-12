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
package org.apache.hive.service.cli.thrift;

import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCancelOperationReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test operation's drilldown link is presented on TStatus when enabled.
 *
 */
public class TestThriftCliServiceWithInfoMessage {
  private HiveServer2 hiveServer2;
  private int cliPort;
  private int webuiPort;
  private String host = "localhost";

  @Before
  public void setUp() throws Exception {
    cliPort = MetaStoreTestUtils.findFreePort();
    webuiPort = MetaStoreTestUtils.findFreePort();
    while (cliPort == webuiPort) {
      webuiPort = MetaStoreTestUtils.findFreePort();
    }
    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_USE_SSL, false);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, cliPort);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthConstants.AuthTypes.NOSASL.toString());
    hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "binary");
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_WEBUI_PORT, webuiPort);
    hiveConf.setBoolVar(ConfVars.HIVE_DEFAULT_NULLS_LAST, true);

    // Enable showing operation drilldown link
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_SHOW_OPERATION_DRILLDOWN_LINK, true);
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    try {
      hiveServer2.start();
    } catch (Throwable t) {
      t.printStackTrace();
      fail();
    }
    // Wait for startup to complete
    Thread.sleep(2000);
  }

  @Test
  public void testExecuteReturnWithInfoMessage() throws Exception {
    TTransport transport = HiveAuthUtils.getSocketTransport(host, cliPort, 0);
    try {
      transport.open();
      TCLIService.Iface client = new TCLIService.Client(new TBinaryProtocol(transport));
      TOpenSessionReq openReq = new TOpenSessionReq();
      openReq.setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11);
      TOpenSessionResp sessionResp = client.OpenSession(openReq);

      Map<String, String> serverHiveConf = sessionResp.getConfiguration();
      assertNotNull(serverHiveConf);
      assertTrue(Boolean.parseBoolean(serverHiveConf.get(ConfVars.HIVE_DEFAULT_NULLS_LAST.varname)));

      TSessionHandle sessHandle = sessionResp.getSessionHandle();
      TExecuteStatementReq execReq = new TExecuteStatementReq(sessHandle, "select 1");
      execReq.setRunAsync(true);
      TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
      TStatus status = execResp.getStatus();
      assertTrue(status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS);
      assertNotNull(status.getInfoMessages());
      assertTrue(status.getInfoMessages().size() > 0);
      Pattern pattern = Pattern.compile("http://.*:(\\d+)/query_page\\.html\\?operationId=(.+)$");
      Matcher matcher = pattern.matcher(status.getInfoMessages().get(0));
      assertTrue(matcher.find());
      assertEquals(webuiPort + "", matcher.group(1));
      OperationHandle operationHandle = new OperationHandle(execResp.getOperationHandle());
      assertEquals(operationHandle.getHandleIdentifier().toString(), matcher.group(2));
      // Cancel the operation
      TCancelOperationReq cancelReq = new TCancelOperationReq();
      cancelReq.setOperationHandle(execResp.getOperationHandle());
      client.CancelOperation(cancelReq);

      // Disable showing the link
      Map<String, String> overlayConf = new HashMap<>();
      overlayConf.put(ConfVars.HIVE_SERVER2_SHOW_OPERATION_DRILLDOWN_LINK.varname, "false");
      execReq = new TExecuteStatementReq(sessHandle, "select 1");
      execReq.setRunAsync(true);
      execReq.setConfOverlay(overlayConf);
      execResp = client.ExecuteStatement(execReq);
      status = execResp.getStatus();
      assertTrue(status.getStatusCode() == TStatusCode.SUCCESS_STATUS);
      assertTrue(status.getInfoMessages() == null);
    } finally {
      transport.close();
    }
  }

  @After
  public void tearDown() {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }

}
