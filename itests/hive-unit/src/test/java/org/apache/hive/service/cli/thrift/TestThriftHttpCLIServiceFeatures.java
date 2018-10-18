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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hive.jdbc.HttpBasicAuthInterceptor;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.auth.HiveAuthConstants.AuthTypes;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.client.CookieStore;
import org.apache.http.client.protocol.RequestDefaultHeaders;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.google.common.base.Joiner;

/**
 *
 * Tests that are specific to HTTP transport mode, that need use of underlying
 * classes instead of jdbc.
 */

public class TestThriftHttpCLIServiceFeatures  {

  private static String transportMode = "http";
  private static String thriftHttpPath = "cliservice";
  static HiveAuthorizer mockedAuthorizer;

  /**
   *  HttpBasicAuthInterceptorWithLogging
   *  This adds httpRequestHeaders to the BasicAuthInterceptor
   */
  public class HttpBasicAuthInterceptorWithLogging extends HttpBasicAuthInterceptor {

    ArrayList<String> requestHeaders;
    String cookieHeader;

    public HttpBasicAuthInterceptorWithLogging(String username, String password,
        CookieStore cookieStore, String cn, boolean isSSL, Map<String, String> additionalHeaders,
        Map<String, String> customCookies) {
      super(username, password, cookieStore, cn, isSSL, additionalHeaders, customCookies);
      requestHeaders = new ArrayList<String>();
    }

    @Override
    public void process(HttpRequest httpRequest, HttpContext httpContext)
        throws HttpException, IOException {
      super.process(httpRequest, httpContext);

      String currHeaders = "";

      for (org.apache.http.Header h : httpRequest.getAllHeaders()) {
        currHeaders += h.getName() + ":" + h.getValue() + " ";
      }
      requestHeaders.add(currHeaders);

      Header[] headers = httpRequest.getHeaders("Cookie");
      cookieHeader = "";
      for (Header h : headers) {
        cookieHeader = cookieHeader + h.getName() + ":" + h.getValue();
      }
    }

    public ArrayList<String> getRequestHeaders() {
      return requestHeaders;
    }

    public String getCookieHeader() {
      return cookieHeader;
    }
  }


  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set up the base class
    ThriftCLIServiceTest.setUpBeforeClass();

    assertNotNull(ThriftCLIServiceTest.port);
    assertNotNull(ThriftCLIServiceTest.hiveServer2);
    assertNotNull(ThriftCLIServiceTest.hiveConf);
    HiveConf hiveConf = ThriftCLIServiceTest.hiveConf;

    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, ThriftCLIServiceTest.host);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT, ThriftCLIServiceTest.port);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, HiveAuthConstants.AuthTypes.NOSASL.toString());
    hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, transportMode);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH, thriftHttpPath);
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    hiveConf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, MockedHiveAuthorizerFactory.class.getName());
    hiveConf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    hiveConf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);

    ThriftCLIServiceTest.startHiveServer2WithConf(hiveConf);

    ThriftCLIServiceTest.client = ThriftCLIServiceTest.getServiceClientInternal();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ThriftCLIServiceTest.tearDownAfterClass();
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
    return HiveAuthUtils.getSocketTransport(ThriftCLIServiceTest.host, ThriftCLIServiceTest.port, 0);
  }

  private static TTransport getHttpTransport() throws Exception {
    DefaultHttpClient httpClient = new DefaultHttpClient();
    String httpUrl = getHttpUrl();
    httpClient.addRequestInterceptor(
        new HttpBasicAuthInterceptor(ThriftCLIServiceTest.USERNAME, ThriftCLIServiceTest.PASSWORD,
            null, null, false, null, null));
    return new THttpClient(httpUrl, httpClient);
  }

  private static String getHttpUrl() {
    return transportMode + "://" + ThriftCLIServiceTest.host + ":"
        + ThriftCLIServiceTest.port +
        "/" + thriftHttpPath + "/";
  }

  /**
   * Test additional http headers passed to request interceptor.
   * @throws Exception
   */
  @Test
  public void testAdditionalHttpHeaders() throws Exception {
    TTransport transport;
    DefaultHttpClient hClient = new DefaultHttpClient();
    String httpUrl = getHttpUrl();
    Map<String, String> additionalHeaders = new HashMap<String, String>();
    additionalHeaders.put("key1", "value1");
    additionalHeaders.put("key2", "value2");
    HttpBasicAuthInterceptorWithLogging authInt =
      new HttpBasicAuthInterceptorWithLogging(ThriftCLIServiceTest.USERNAME, ThriftCLIServiceTest.PASSWORD, null, null,
      false, additionalHeaders, null);
    hClient.addRequestInterceptor(authInt);
    transport = new THttpClient(httpUrl, hClient);
    TCLIService.Client httpClient = getClient(transport);

    // Create a new open session request object
    TOpenSessionReq openReq = new TOpenSessionReq();
    httpClient.OpenSession(openReq).getSessionHandle();
    ArrayList<String> headers = authInt.getRequestHeaders();

    for (String h : headers) {
      assertTrue(h.contains("key1:value1"));
      assertTrue(h.contains("key2:value2"));
    }
  }

  /**
   * Test additional http headers passed to request interceptor.
   * @throws Exception
   */
  @Test
  public void testCustomCookies() throws Exception {
    TTransport transport;
    DefaultHttpClient hClient = new DefaultHttpClient();
    String httpUrl = getHttpUrl();
    Map<String, String> additionalHeaders = new HashMap<String, String>();
    Map<String, String> cookieHeaders = new HashMap<String, String>();
    cookieHeaders.put("key1", "value1");
    cookieHeaders.put("key2", "value2");
    HttpBasicAuthInterceptorWithLogging authInt =
      new HttpBasicAuthInterceptorWithLogging(ThriftCLIServiceTest.USERNAME, ThriftCLIServiceTest.PASSWORD, null, null,
      false, additionalHeaders, cookieHeaders);
    hClient.addRequestInterceptor(authInt);
    transport = new THttpClient(httpUrl, hClient);
    TCLIService.Client httpClient = getClient(transport);

    // Create a new open session request object
    TOpenSessionReq openReq = new TOpenSessionReq();
    httpClient.OpenSession(openReq).getSessionHandle();
    String cookieHeader = authInt.getCookieHeader();
    assertTrue(cookieHeader.contains("key1=value1"));
    assertTrue(cookieHeader.contains("key2=value2"));
  }


  /**
   * This factory creates a mocked HiveAuthorizer class.
   * Use the mocked class to capture the argument passed to it in the test case.
   */
  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      mockedAuthorizer = Mockito.mock(HiveAuthorizer.class);
      return mockedAuthorizer;
    }
  }

  /**
   * Test if addresses in X-Forwarded-For are passed to HiveAuthorizer calls
   * @throws Exception
   */
  @Test
  public void testForwardedHeaders() throws Exception {
    verifyForwardedHeaders(new ArrayList<String>(Arrays.asList("127.0.0.1", "202.101.101.101")), "show tables");
    verifyForwardedHeaders(new ArrayList<String>(Arrays.asList("202.101.101.101")), "fs -ls /");
    verifyForwardedHeaders(new ArrayList<String>(), "show databases");
  }

  private void verifyForwardedHeaders(ArrayList<String> headerIPs, String cmd) throws Exception {
    TTransport transport;
    DefaultHttpClient hClient = new DefaultHttpClient();
    String httpUrl = getHttpUrl();

    // add an interceptor that adds the X-Forwarded-For header with given ips
    if (!headerIPs.isEmpty()) {
      Header xForwardHeader = new BasicHeader("X-Forwarded-For", Joiner.on(",").join(headerIPs));
      RequestDefaultHeaders headerInterceptor = new RequestDefaultHeaders(
          Arrays.asList(xForwardHeader));
      hClient.addRequestInterceptor(headerInterceptor);
    }

    // interceptor for adding username, pwd
    HttpBasicAuthInterceptor authInt = new HttpBasicAuthInterceptor(ThriftCLIServiceTest.USERNAME,
        ThriftCLIServiceTest.PASSWORD, null, null,
        false, null, null);
    hClient.addRequestInterceptor(authInt);

    transport = new THttpClient(httpUrl, hClient);
    TCLIService.Client httpClient = getClient(transport);

    // Create a new open session request object
    TOpenSessionReq openReq = new TOpenSessionReq();
    TOpenSessionResp openResp = httpClient.OpenSession(openReq);

    //execute a query
    TExecuteStatementReq execReq = new TExecuteStatementReq(openResp.getSessionHandle(), "show tables");
    httpClient.ExecuteStatement(execReq);

    // capture arguments to authorizer impl call and verify ip addresses passed
    ArgumentCaptor<HiveAuthzContext> contextCapturer = ArgumentCaptor
        .forClass(HiveAuthzContext.class);

    verify(mockedAuthorizer).checkPrivileges(any(HiveOperationType.class),
        Matchers.anyListOf(HivePrivilegeObject.class),
        Matchers.anyListOf(HivePrivilegeObject.class), contextCapturer.capture());

    HiveAuthzContext context = contextCapturer.getValue();
    System.err.println("Forwarded IP Addresses " + context.getForwardedAddresses());

    List<String> auditIPAddresses = new ArrayList<String>(context.getForwardedAddresses());
    Collections.sort(auditIPAddresses);
    Collections.sort(headerIPs);

    Assert.assertEquals("Checking forwarded IP Address" , headerIPs, auditIPAddresses);
  }


}
