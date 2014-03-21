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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.HttpBasicAuthInterceptor;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
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

    client = getServiceClientInternal();
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

  }

  /**
   * @throws java.lang.Exception
   */
  @Override
  @After
  public void tearDown() throws Exception {

  }

  @Test
  /**
   * Tests calls from a raw (NOSASL) binary client,
   * to a HiveServer2 running in http mode.
   * This should throw an expected exception due to incompatibility.
   * @throws Exception
   */
  public void testBinaryClientHttpServer() throws Exception {
    TTransport transport = getRawBinaryTransport();
    TCLIService.Client rawBinaryClient = getClient(transport);

    // This will throw an expected exception since client-server modes are incompatible
    testOpenSessionExpectedException(rawBinaryClient);
  }

  /**
   * Configure a wrong service endpoint for the client transport,
   * and test for error.
   * @throws Exception
   */
  @Test
  public void testIncorrectHttpPath() throws Exception {
    thriftHttpPath = "wrongPath";
    TTransport transport = getHttpTransport();
    TCLIService.Client httpClient = getClient(transport);

    // This will throw an expected exception since
    // client is communicating with the wrong http service endpoint
    testOpenSessionExpectedException(httpClient);

    // Reset to correct http path
    thriftHttpPath = "cliservice";
  }

  private void testOpenSessionExpectedException(TCLIService.Client client) {
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

  private TCLIService.Client getClient(TTransport transport) throws Exception {
    // Create the corresponding client
    TProtocol protocol = new TBinaryProtocol(transport);
    return new TCLIService.Client(protocol);
  }

  private TTransport getRawBinaryTransport() throws Exception {
    return HiveAuthFactory.getSocketTransport(host, port, 0);
  }

  private static TTransport getHttpTransport() throws Exception {
    DefaultHttpClient httpClient = new DefaultHttpClient();
    String httpUrl = transportMode + "://" + host + ":" + port +
        "/" + thriftHttpPath + "/";
    httpClient.addRequestInterceptor(
        new HttpBasicAuthInterceptor(USERNAME, PASSWORD));
    return new THttpClient(httpUrl, httpClient);
  }

}