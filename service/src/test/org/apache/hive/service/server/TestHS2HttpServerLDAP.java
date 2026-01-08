/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.server;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hive.service.auth.HttpAuthService;
import org.apache.hive.service.auth.HttpAuthUtils;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.CookieStore;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.eclipse.jetty.http.HttpHeader;
import java.util.Base64;
import java.nio.charset.StandardCharsets;
import org.eclipse.jetty.util.StringUtil;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.sasl.AuthenticationException;

public class TestHS2HttpServerLDAP {

  private static HiveServer2 hiveServer2;
  private static Integer webUIPort;
  private static final String HOST = "localhost";
  private static final String METASTORE_PASSWD = "693efe9fa425ad21886d73a0fa3fbc70"; //random md5
  private static final String VALID_USER = "validUser";
  private static final String VALID_PASS = "validPass";
  private static final String INVALID_USER = "invalidUser";
  private static final String INVALID_PASS = "invalidPass";

  @BeforeClass
  public static void beforeTests() throws Exception {
    webUIPort =
        MetaStoreTestUtils.findFreePortExcepting(Integer.parseInt(ConfVars.HIVE_SERVER2_WEBUI_PORT.getDefaultValue()));
    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(ConfVars.HIVE_IN_TEST, true);
    hiveConf.set(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname, webUIPort.toString());
    hiveConf.set(ConfVars.HIVE_SERVER2_WEBUI_AUTH_METHOD.varname, "LDAP");
    hiveConf.set(ConfVars.METASTORE_PWD.varname, METASTORE_PASSWD);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    // query history adds no value to this test, it would just bring iceberg handler dependency, which isn't worth
    // this should be handled with HiveConfForTests when it's used here too
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_QUERY_HISTORY_ENABLED, false);
    PasswdAuthenticationProvider authenticationProvider = new DummyLdapAuthenticationProviderImpl();
    hiveServer2 = new HiveServer2(authenticationProvider);
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    Thread.sleep(5000);
  }

  @Test
  public void testValidCredentialsWithAuthorizationHeader() throws Exception {
    CloseableHttpClient httpclient = null;
    try {
      CookieStore httpCookieStore = new BasicCookieStore();
      HttpClientBuilder builder = HttpClientBuilder.create().setDefaultCookieStore(httpCookieStore);
      httpclient = builder.build();

      HttpGet httpGet = new HttpGet("http://" + HOST + ":" + webUIPort + "/jmx");
      String credentials = VALID_USER + ":" + VALID_PASS;
      String authBase64Code = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.ISO_8859_1));
      httpGet.setHeader(HttpHeader.AUTHORIZATION.asString(), "Basic " + authBase64Code);
      httpclient.execute(httpGet);

      Assert.assertTrue(isAuthorized(httpCookieStore.getCookies()));
    } finally {
      if (httpclient != null) {
        httpclient.close();
      }
    }
  }

  @Test
  public void testInvalidCredentialsWithInAuthorizationHeader() throws Exception {
    CloseableHttpClient httpclient = null;
    try {
      CookieStore httpCookieStore = new BasicCookieStore();
      HttpClientBuilder builder = HttpClientBuilder.create().setDefaultCookieStore(httpCookieStore);
      httpclient = builder.build();

      HttpGet httpGet = new HttpGet("http://" + HOST + ":" + webUIPort + "/jmx");
      String credentials = INVALID_USER + ":" + INVALID_PASS;
      String authBase64Code = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.ISO_8859_1));
      httpGet.setHeader(HttpHeader.AUTHORIZATION.asString(), "Basic " + authBase64Code);
      httpclient.execute(httpGet);

      Assert.assertFalse(isAuthorized(httpCookieStore.getCookies()));
    } finally {
      if (httpclient != null) {
        httpclient.close();
      }
    }
  }

  @Test
  public void testValidCredentialsWithRequestParameters() throws Exception {
    CloseableHttpClient httpclient = null;
    try {
      CookieStore httpCookieStore = new BasicCookieStore();
      HttpClientBuilder builder = HttpClientBuilder.create().setDefaultCookieStore(httpCookieStore);
      httpclient = builder.build();

      HttpParams params = new BasicHttpParams();
      params.setParameter(HttpAuthService.USERNAME_REQUEST_PARAM_NAME, VALID_USER);
      params.setParameter(HttpAuthService.PASSWORD_REQUEST_PARAM_NAME, VALID_PASS);

      HttpPost httpPost = new HttpPost("http://" + HOST + ":" + webUIPort + "/login");
      httpPost.setParams(params);
      httpclient.execute(httpPost);

      Assert.assertFalse(isAuthorized(httpCookieStore.getCookies()));
    } finally {
      if (httpclient != null) {
        httpclient.close();
      }
    }
  }

  @Test
  public void testInvalidCredentialsWithRequestParameters() throws Exception {
    CloseableHttpClient httpclient = null;
    try {
      CookieStore httpCookieStore = new BasicCookieStore();
      HttpClientBuilder builder = HttpClientBuilder.create().setDefaultCookieStore(httpCookieStore);
      httpclient = builder.build();

      HttpParams params = new BasicHttpParams();
      params.setParameter(HttpAuthService.USERNAME_REQUEST_PARAM_NAME, INVALID_USER);
      params.setParameter(HttpAuthService.PASSWORD_REQUEST_PARAM_NAME, INVALID_PASS);

      HttpPost httpPost = new HttpPost("http://" + HOST + ":" + webUIPort + "/login");
      httpPost.setParams(params);
      httpclient.execute(httpPost);

      Assert.assertFalse(isAuthorized(httpCookieStore.getCookies()));
    } finally {
      if (httpclient != null) {
        httpclient.close();
      }
    }
  }

  @Test
  public void testEscapeAuthentication() throws Exception {
    // Verify any un-authenticated requests are forwarding to the login page
    try (CloseableHttpClient httpclient = HttpClientBuilder.create().build()) {
      try (CloseableHttpResponse response =
               httpclient.execute(new HttpGet("http://" + HOST + ":" + webUIPort + "/hiveserver2.jsp;login"))) {
        checkForwardToLoginPage(response);
      }
      try (CloseableHttpResponse response =
               httpclient.execute(new HttpGet("http://" + HOST + ":" + webUIPort + "/logs"))) {
        checkForwardToLoginPage(response);
      }
    }
  }

  private void checkForwardToLoginPage(CloseableHttpResponse response) throws Exception {
    Assert.assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
    String content = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
    Assert.assertTrue(content.contains("<meta name=\"description\" content=\"Login - Hive\">"));
    Assert.assertTrue(content.contains("<div class=\"login-form\">"));
  }

  public static boolean isAuthorized(List<Cookie> cookies) {
    Optional<Cookie> cookie = cookies.stream()
        .filter(c -> c.getName().equals(HttpAuthService.HIVE_SERVER2_WEBUI_AUTH_COOKIE_NAME))
        .findAny();
    
    if (!cookie.isPresent()) {
      return false;
    }

    String signedCookieValue = cookie.get().getValue();
    String cookieValue = hiveServer2.getLdapAuthService().verifyAndExtract(signedCookieValue);
    String userName = HttpAuthUtils.getUserNameFromCookieToken(cookieValue);

    return userName.equals(VALID_USER);
  }
  
  @AfterClass 
  public static void afterTests() {
    hiveServer2.stop();
  }
  
  public static class DummyLdapAuthenticationProviderImpl implements PasswdAuthenticationProvider {
    @Override
    public void authenticate(String user, String password) throws AuthenticationException {
      if (!(user.equals(VALID_USER) && password.equals(VALID_PASS)))
        throw new AuthenticationException();
    }
  }
}
