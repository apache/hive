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
package org.apache.hive.service.cli.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Collection;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hive.service.Service;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.server.HiveServer2;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * ThriftCLIServiceTest.
 * This is the abstract class that tests ThriftCLIService.
 * Subclass this to test more specific behaviour.
 *
 */
public abstract class ThriftCLIServiceTest {

  protected static int port;
  protected static String host = "localhost";
  protected static HiveServer2 hiveServer2;
  protected static TCLIService.Client client;
  protected static HiveConf hiveConf;
  protected static String anonymousUser = "anonymous";
  protected static String anonymousPasswd = "anonymous";



  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Find a free port
    port = MetaStoreUtils.findFreePort();
    hiveServer2 = new HiveServer2();
    hiveConf = new HiveConf();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    stopHiveServer2();
  }

  protected static void startHiveServer2WithConf(HiveConf hiveConf) throws Exception {
    hiveServer2.init(hiveConf);
    // Start HiveServer2 with given config
    // Fail if server doesn't start
    try {
      hiveServer2.start();
    } catch (Throwable t) {
      t.printStackTrace();
      fail();
    }
    // Wait for startup to complete
    Thread.sleep(2000);
    System.out.println("HiveServer2 started on port " + port);
  }

  protected static void stopHiveServer2() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }

  protected static TTransport createBinaryTransport() throws Exception {
    return PlainSaslHelper.getPlainTransport(anonymousUser, anonymousPasswd,
        new TSocket(host, port));
  }

  protected static void initClient(TTransport transport) {
    // Create the corresponding client
    TProtocol protocol = new TBinaryProtocol(transport);
    client = new TCLIService.Client(protocol);
  }

  @Test
  public void testOpenSession() throws Exception {
    // Create a new request object
    TOpenSessionReq openReq = new TOpenSessionReq();

    // Get the response; ignore exception if any
    TOpenSessionResp openResp = client.OpenSession(openReq);
    assertNotNull("Response should not be null", openResp);

    TSessionHandle sessHandle = openResp.getSessionHandle();
    assertNotNull("Session handle should not be null", sessHandle);

    assertEquals(openResp.getStatus().getStatusCode(), TStatusCode.SUCCESS_STATUS);

    // Close the session; ignore exception if any
    TCloseSessionReq closeReq = new TCloseSessionReq(sessHandle);
    client.CloseSession(closeReq);
  }

  @Test
  public void testGetFunctions() throws Exception {
    // Create a new open session request object
    TOpenSessionReq openReq = new TOpenSessionReq();
    TSessionHandle sessHandle = client.OpenSession(openReq).getSessionHandle();
    assertNotNull(sessHandle);

    TGetFunctionsReq funcReq = new TGetFunctionsReq();
    funcReq.setSessionHandle(sessHandle);
    funcReq.setFunctionName("*");
    funcReq.setCatalogName(null);
    funcReq.setSchemaName(null);

    TGetFunctionsResp funcResp = client.GetFunctions(funcReq);
    assertNotNull(funcResp);
    assertNotNull(funcResp.getStatus());
    assertFalse(funcResp.getStatus().getStatusCode() == TStatusCode.ERROR_STATUS);

    // Close the session; ignore exception if any
    TCloseSessionReq closeReq = new TCloseSessionReq(sessHandle);
    client.CloseSession(closeReq);
  }

  @Test
  public void testExecuteStatement() throws Exception {
    // Create a new request object
    TOpenSessionReq openReq = new TOpenSessionReq();
    TSessionHandle sessHandle = client.OpenSession(openReq).getSessionHandle();
    assertNotNull(sessHandle);

    // Change lock manager to embedded mode
    String queryString = "SET hive.lock.manager=" +
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager";
    executeQuerySync(queryString, sessHandle);

    // Drop the table if it exists
    queryString = "DROP TABLE IF EXISTS TEST_EXEC_THRIFT";
    executeQuerySync(queryString, sessHandle);

    // Create a test table
    queryString = "CREATE TABLE TEST_EXEC_THRIFT(ID STRING)";
    executeQuerySync(queryString, sessHandle);

    // Execute another query to test
    queryString = "SELECT ID FROM TEST_EXEC_THRIFT";
    TExecuteStatementResp execResp = executeQuerySync(queryString, sessHandle);
    TOperationHandle operationHandle = execResp.getOperationHandle();
    assertNotNull(operationHandle);

    TGetOperationStatusReq opStatusReq = new TGetOperationStatusReq();
    opStatusReq.setOperationHandle(operationHandle);
    assertNotNull(opStatusReq);
    TGetOperationStatusResp opStatusResp = client.GetOperationStatus(opStatusReq);

    // Expect query to be completed now
    assertEquals("Query should be finished",
        OperationState.FINISHED, OperationState.getOperationState(opStatusResp.getOperationState()));

    queryString = "DROP TABLE TEST_EXEC_THRIFT";
    executeQuerySync(queryString, sessHandle);

    // Close the session; ignore exception if any
    TCloseSessionReq closeReq = new TCloseSessionReq(sessHandle);
    client.CloseSession(closeReq);
  }

  private TExecuteStatementResp executeQuerySync(String queryString, TSessionHandle sessHandle)
      throws Exception {
    TExecuteStatementReq execReq = new TExecuteStatementReq();
    execReq.setSessionHandle(sessHandle);
    execReq.setStatement(queryString);
    execReq.setRunAsync(false);
    TExecuteStatementResp execResp = client.ExecuteStatement(execReq);
    assertNotNull(execResp);
    assertFalse(execResp.getStatus().getStatusCode() == TStatusCode.ERROR_STATUS);
    return execResp;
  }

  protected void testOpenSessionExpectedException() {
    boolean caughtEx = false;
    // Create a new open session request object
    TOpenSessionReq openReq = new TOpenSessionReq();
    try {
      client.OpenSession(openReq).getSessionHandle();
    } catch (Exception e) {
      caughtEx = true;
      System.out.println("Exception expected: " + e.toString());
    }
    assertTrue("Exception expected", caughtEx);
  }

  /**
   * Test setting {@link HiveConf.ConfVars}} config parameter
   *   HIVE_SERVER2_ENABLE_DOAS for kerberos secure mode
   * @throws IOException
   * @throws LoginException
   * @throws HiveSQLException
   */
  @Test
  public void testDoAs() throws HiveSQLException, LoginException, IOException {
    HiveConf hconf = new HiveConf();
    assertTrue("default value of hive server2 doAs should be true",
        hconf.getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS));

    hconf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION,
        HiveAuthFactory.AuthTypes.KERBEROS.toString());

    CLIService cliService = new CLIService();
    cliService.init(hconf);
    ThriftCLIService tcliService = new ThriftBinaryCLIService(cliService);
    TOpenSessionReq req = new TOpenSessionReq();
    req.setUsername("testuser1");
    SessionHandle sHandle = tcliService.getSessionHandle(req );
    SessionManager sManager = getSessionManager(cliService.getServices());
    HiveSession session = sManager.getSession(sHandle);

    //Proxy class for doing doAs on all calls is used when doAs is enabled
    // and kerberos security is on
    assertTrue("check if session class is a proxy", session instanceof java.lang.reflect.Proxy);
  }

  private SessionManager getSessionManager(Collection<Service> services) {
    for(Service s : services){
      if(s instanceof SessionManager){
        return (SessionManager)s;
      }
    }
    return null;
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {

  }
}
