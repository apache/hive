

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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hive.service.Service;
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.HiveServer2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * ThriftCLIServiceTestWithCookie.
 */
public class ThriftCliServiceTestWithCookie {

  protected static int port;
  protected static String host = "localhost";
  protected static HiveServer2 hiveServer2;
  protected static ThriftCLIServiceClient client;
  protected static HiveConf hiveConf;
  protected static String USERNAME = "anonymous";
  protected static String PASSWORD = "anonymous";

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Find a free port
    port = MetaStoreUtils.findFreePort();
    hiveServer2 = new HiveServer2();
    hiveConf = new HiveConf();
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_AUTH_ENABLED, true);
    // Set the cookie max age to a very low value so that
    // the server sends 401 very frequently
    hiveConf.setTimeVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_MAX_AGE, 1, TimeUnit.SECONDS);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, "http");
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH, "cliservice");

    assertNotNull(port);
    assertNotNull(hiveServer2);
    assertNotNull(hiveConf);

    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT, port);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, AuthTypes.NOSASL.toString());

    startHiveServer2WithConf(hiveConf);

    client = getServiceClientInternal();
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

  protected static ThriftCLIServiceClient getServiceClientInternal() {
    for (Service service : hiveServer2.getServices()) {
      if (service instanceof ThriftBinaryCLIService) {
        return new ThriftCLIServiceClient((ThriftBinaryCLIService) service);
      }
      if (service instanceof ThriftHttpCLIService) {
        return new ThriftCLIServiceClient((ThriftHttpCLIService) service);
      }
    }
    throw new IllegalStateException("HiveServer2 not running Thrift service");
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

  @Test
  public void testOpenSession() throws Exception {
    // Open a new client session
    SessionHandle sessHandle = client.openSession(USERNAME,
        PASSWORD, new HashMap<String, String>());
    // Session handle should not be null
    assertNotNull("Session handle should not be null", sessHandle);
    // Close client session
    client.closeSession(sessHandle);
  }

  @Test
  public void testGetFunctions() throws Exception {
    SessionHandle sessHandle = client.openSession(USERNAME,
        PASSWORD, new HashMap<String, String>());
    assertNotNull("Session handle should not be null", sessHandle);

    String catalogName = null;
    String schemaName = null;
    String functionName = "*";

    OperationHandle opHandle = client.getFunctions(sessHandle, catalogName,
        schemaName, functionName);

    assertNotNull("Operation handle should not be null", opHandle);

    client.closeSession(sessHandle);
  }

  /**
   * Test synchronous query execution
   * @throws Exception
   */
  @Test
  public void testExecuteStatement() throws Exception {
    Map<String, String> opConf = new HashMap<String, String>();
    // Open a new client session
    SessionHandle sessHandle = client.openSession(USERNAME,
        PASSWORD, opConf);
    // Session handle should not be null
    assertNotNull("Session handle should not be null", sessHandle);

    // Change lock manager to embedded mode
    String queryString = "SET hive.lock.manager=" +
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager";
    client.executeStatement(sessHandle, queryString, opConf);

    // Drop the table if it exists
    queryString = "DROP TABLE IF EXISTS TEST_EXEC_THRIFT";
    client.executeStatement(sessHandle, queryString, opConf);

    // Create a test table
    queryString = "CREATE TABLE TEST_EXEC_THRIFT(ID STRING)";
    client.executeStatement(sessHandle, queryString, opConf);

    // Execute another query
    queryString = "SELECT ID+1 FROM TEST_EXEC_THRIFT";
    OperationHandle opHandle = client.executeStatement(sessHandle, queryString, opConf);
    assertNotNull(opHandle);

    OperationStatus opStatus = client.getOperationStatus(opHandle, false);
    assertNotNull(opStatus);

    OperationState state = opStatus.getState();
    // Expect query to be completed now
    assertEquals("Query should be finished", OperationState.FINISHED, state);

    // Cleanup
    queryString = "DROP TABLE TEST_EXEC_THRIFT";
    client.executeStatement(sessHandle, queryString, opConf);

    client.closeSession(sessHandle);
  }
}

