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

package org.apache.hive.service.cli;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.Service;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.thrift.RetryingThriftCLIServiceClient;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Test CLI service with a retrying client. All tests should pass. This is to validate that calls
 * are transferred successfully.
 */
public class TestRetryingThriftCLIServiceClient {
  protected static ThriftCLIService service;
  private HiveConf hiveConf;
  private HiveServer2 server;

  @Before
  public void init() {
    hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, "localhost");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, 15000);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthConstants.AuthTypes.NONE.toString());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "binary");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT, 3);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT, 3);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_THREADS, 10);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_ASYNC_EXEC_SHUTDOWN_TIMEOUT, "1s");
    hiveConf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
  }

  private void startHiveServer() throws InterruptedException {
    // Start hive server2
    server = new HiveServer2();
    server.init(hiveConf);
    server.start();
    Thread.sleep(5000);
    System.out.println("## HiveServer started");
  }

  private void stopHiveServer() {
    if (server != null) {
      // kill server
      server.stop();
    }
  }

  static class RetryingThriftCLIServiceClientTest extends RetryingThriftCLIServiceClient {
    int callCount = 0;
    int connectCount = 0;
    static RetryingThriftCLIServiceClientTest handlerInst;

    protected RetryingThriftCLIServiceClientTest(HiveConf conf) {
      super(conf);
    }

    public static CLIServiceClientWrapper newRetryingCLIServiceClient(HiveConf conf) throws HiveSQLException {
      handlerInst = new RetryingThriftCLIServiceClientTest(conf);
      TTransport tTransport
        = handlerInst.connectWithRetry(conf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_RETRY_LIMIT));
      ICLIService cliService =
        (ICLIService) Proxy.newProxyInstance(RetryingThriftCLIServiceClientTest.class.getClassLoader(),
          CLIServiceClient.class.getInterfaces(), handlerInst);
      return new CLIServiceClientWrapper(cliService, tTransport, conf);
    }

    @Override
    protected InvocationResult invokeInternal(Method method, Object[] args) throws Throwable {
      System.out.println("## Calling: " + method.getName() + ", " + callCount + "/" + getRetryLimit());
      callCount++;
      return super.invokeInternal(method, args);
    }

    @Override
    protected synchronized TTransport connect(HiveConf conf) throws HiveSQLException, TTransportException {
      connectCount++;
      return super.connect(conf);
    }
  }

  @Test
  public void testRetryBehaviour() throws Exception {
    startHiveServer();
    // Check if giving invalid address causes retry in connection attempt
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, 17000);
    try {
      RetryingThriftCLIServiceClientTest.newRetryingCLIServiceClient(hiveConf);
      fail("Expected to throw exception for invalid port");
    } catch (HiveSQLException sqlExc) {
      assertTrue(sqlExc.getCause() instanceof TTransportException);
      assertTrue(sqlExc.getMessage().contains("3"));
    }

    // Reset port setting
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, 15000);
    // Create client
    RetryingThriftCLIServiceClient.CLIServiceClientWrapper cliServiceClient
      = RetryingThriftCLIServiceClientTest.newRetryingCLIServiceClient(hiveConf);
    System.out.println("## Created client");

    stopHiveServer();
    Thread.sleep(5000);

    // submit few queries
    try {
      RetryingThriftCLIServiceClientTest.handlerInst.callCount = 0;
      RetryingThriftCLIServiceClientTest.handlerInst.connectCount = 0;
      cliServiceClient.openSession("anonymous", "anonymous");
    } catch (HiveSQLException exc) {
      exc.printStackTrace();
      assertTrue(exc.getCause() instanceof TException);
      assertEquals(1, RetryingThriftCLIServiceClientTest.handlerInst.callCount);
      assertEquals(3, RetryingThriftCLIServiceClientTest.handlerInst.connectCount);
    } finally {
      cliServiceClient.closeTransport();
    }
  }

  @Test
  public void testTransportClose() throws InterruptedException, HiveSQLException {
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT, 0);
    try {
      startHiveServer();
      RetryingThriftCLIServiceClient.CLIServiceClientWrapper client
        = RetryingThriftCLIServiceClientTest.newRetryingCLIServiceClient(hiveConf);
      client.closeTransport();
      try {
        client.openSession("anonymous", "anonymous");
        fail("Shouldn't be able to open session when transport is closed.");
      } catch(HiveSQLException ignored) {

      }
    } finally {
      hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_CLIENT_CONNECTION_RETRY_LIMIT, 3);
      stopHiveServer();
    }
  }

  @Test
  public void testSessionLifeAfterTransportClose() throws InterruptedException, HiveSQLException {
    try {
      startHiveServer();
      CLIService service = null;
      for (Service s : server.getServices()) {
        if (s instanceof CLIService) {
          service = (CLIService) s;
        }
      }
      if (service == null) {
        service = new CLIService(server);
      }
      RetryingThriftCLIServiceClient.CLIServiceClientWrapper client
        = RetryingThriftCLIServiceClientTest.newRetryingCLIServiceClient(hiveConf);
      Map<String, String> conf = new HashMap<>();
      conf.put(HiveConf.ConfVars.HIVE_SERVER2_CLOSE_SESSION_ON_DISCONNECT.varname, "false");
      SessionHandle sessionHandle = client.openSession("anonymous", "anonymous", conf);
      assertNotNull(sessionHandle);
      HiveSession session = service.getSessionManager().getSession(sessionHandle);
      OperationHandle op1 = session.executeStatementAsync("show databases", null);
      assertNotNull(op1);
      client.closeTransport();
      // Verify that session wasn't closed on transport close.
      assertEquals(session, service.getSessionManager().getSession(sessionHandle));
      // Should be able to execute without failure in the session whose transport has been closed.
      OperationHandle op2 = session.executeStatementAsync("show databases", null);
      assertNotNull(op2);
      // Make new client, since transport was closed for the last one.
      client = RetryingThriftCLIServiceClientTest.newRetryingCLIServiceClient(hiveConf);
      client.closeSession(sessionHandle);
      // operations will be lost once owning session is closed.
      for (OperationHandle op: new OperationHandle[]{op1, op2}) {
        try {
          client.getOperationStatus(op, false);
          fail("Should have failed.");
        } catch (HiveSQLException ignored) {

        }
      }
    } finally {
      stopHiveServer();
    }
  }
}
