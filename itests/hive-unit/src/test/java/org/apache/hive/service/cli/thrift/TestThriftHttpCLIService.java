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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.HttpBasicAuthInterceptor;
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes;
import org.apache.hive.service.server.HiveServer2;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
*
* TestThriftHttpCLIService.
* This tests ThriftCLIService started in http mode.
*
*/

public class TestThriftHttpCLIService extends ThriftCLIServiceTest {

  private static String transportMode = "http";
  private static String thriftHttpPath = "cliservice";
  private static TTransport transport;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set up the base class
    ThriftCLIServiceTest.setUpBeforeClass();

    assertNotNull(port);
    assertNotNull(hiveServer2);
    assertNotNull(hiveConf);

    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT, port);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, AuthTypes.NOSASL.toString());
    hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, transportMode);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH, thriftHttpPath);

    startHiveServer2WithConf(hiveConf);

    // Open an http transport
    // Fail if the transport doesn't open
    transport = createHttpTransport();
    try {
      transport.open();
    }
    catch (Exception e) {
      fail("Exception: " + e);
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ThriftCLIServiceTest.tearDownAfterClass();
  }

  /**
   * @throws java.lang.Exception
   */
  @Override
  @Before
  public void setUp() throws Exception {
    // Create and set the client before every test from the transport
    initClient(transport);
    assertNotNull(client);
  }

  /**
   * @throws java.lang.Exception
   */
  @Override
  @After
  public void tearDown() throws Exception {

  }

  @Test
  public void testIncompatibeClientServer() throws Exception {
    // A binary client communicating with an http server should throw an exception
    // Close the older http client transport
    // The server is already running in Http mode
    if (transport != null) {
      transport.close();
    }
    // Create a binary transport and init the client
    transport = createBinaryTransport();
    // Create and set the client
    initClient(transport);
    assertNotNull(client);

    // This will throw an expected exception since client-server modes are incompatible
    testOpenSessionExpectedException();

    // Close binary client transport
    if (transport != null) {
      transport.close();
    }
    // Create http transport (client is inited in setUp before every test from the transport)
    transport = createHttpTransport();
    try {
      transport.open();
    }
    catch (Exception e) {
      fail("Exception: " + e);
    }
  }

  @Test
  public void testIncorrectHttpPath() throws Exception {
    // Close the older http client transport
    if (transport != null) {
      transport.close();
    }
    // Create an http transport with incorrect http path endpoint
    thriftHttpPath = "wrong_path";
    transport = createHttpTransport();
    // Create and set the client
    initClient(transport);
    assertNotNull(client);

    // This will throw an expected exception since
    // client is communicating with the wrong http service endpoint
    testOpenSessionExpectedException();

    // Close incorrect client transport
    // Reinit http client transport
    thriftHttpPath = "cliservice";
    if (transport != null) {
      transport.close();
    }
    transport = createHttpTransport();
    try {
      transport.open();
    }
    catch (Exception e) {
      fail("Exception: " + e);
    }
  }


  private void testWithAuthMode(AuthTypes authType) throws Exception {
    // Stop and restart HiveServer2 in given incorrect auth mode
    stopHiveServer2();
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, authType.toString());
    hiveServer2 = new HiveServer2();
    // HiveServer2 in Http mode will not start using KERBEROS/LDAP/CUSTOM auth types
    startHiveServer2WithConf(hiveConf);

    // This will throw an expected exception since Http server is not running
    testOpenSessionExpectedException();

    // Stop and restart back with the original config
    stopHiveServer2();
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, AuthTypes.NOSASL.toString());
    hiveServer2 = new HiveServer2();
    startHiveServer2WithConf(hiveConf);
  }

  @Test
  public void testKerberosMode()  throws Exception {
    testWithAuthMode(AuthTypes.KERBEROS);
  }

  @Test
  public void testLDAPMode()  throws Exception {
    testWithAuthMode(AuthTypes.LDAP);
  }

  @Test
  public void testCustomMode()  throws Exception {
    testWithAuthMode(AuthTypes.CUSTOM);
  }

  private static TTransport createHttpTransport() throws Exception {
    DefaultHttpClient httpClient = new DefaultHttpClient();
    String httpUrl = transportMode + "://" + host + ":" + port +
        "/" + thriftHttpPath + "/";
    httpClient.addRequestInterceptor(
        new HttpBasicAuthInterceptor(anonymousUser, anonymousPasswd));
    return new THttpClient(httpUrl, httpClient);
  }

}