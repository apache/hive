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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hive.http.security.PamAuthenticator;
import org.apache.hive.http.security.PamUserIdentity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.B64Code;
import org.eclipse.jetty.util.StringUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.security.sasl.AuthenticationException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * TestHS2HttpServerPam -- executes tests of HiveServer2 HTTP Server for Pam authentication
 */
public class TestHS2HttpServerPam {

  private static HiveServer2 hiveServer2 = null;
  private static HiveConf hiveConf = null;
  private static String metastorePasswd = "693efe9fa425ad21886d73a0fa3fbc70"; //random md5
  private static Integer webUIPort = null;
  private static String host = "localhost";

  @BeforeClass
  public static void beforeTests() throws Exception {
    webUIPort =
        MetaStoreTestUtils.findFreePortExcepting(Integer.valueOf(ConfVars.HIVE_SERVER2_WEBUI_PORT.getDefaultValue()));
    hiveConf = new HiveConf();
    hiveConf.setBoolVar(ConfVars.HIVE_IN_TEST, true);
    hiveConf.set(ConfVars.METASTOREPWD.varname, metastorePasswd);
    hiveConf.set(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname, webUIPort.toString());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PAM_SERVICES, "sshd");
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_WEBUI_USE_PAM, true);
    hiveConf.setBoolVar(ConfVars.HIVE_IN_TEST, true);
    hiveServer2 = new HiveServer2(new TestPamAuthenticator(hiveConf));
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    Thread.sleep(5000);
  }

  @Test
  public void testUnauthorizedConnection() throws Exception {
    String baseURL = "http://" + host + ":" + webUIPort + "/stacks";
    URL url = new URL(baseURL);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, conn.getResponseCode());
  }

  @Test
  public void testAuthorizedConnection() throws Exception {
    CloseableHttpClient httpclient = null;
    try {
      String username = "user1";
      String password = "1";
      httpclient = HttpClients.createDefault();

      HttpGet httpGet = new HttpGet("http://" + host + ":" + webUIPort);
      String authB64Code = B64Code.encode(username + ":" + password, StringUtil.__ISO_8859_1);
      httpGet.setHeader(HttpHeader.AUTHORIZATION.asString(), "Basic " + authB64Code);
      CloseableHttpResponse response = httpclient.execute(httpGet);
      Assert.assertTrue(response.toString().contains(Integer.toString(HttpURLConnection.HTTP_OK)));

    } finally {
      if (httpclient != null) {
        httpclient.close();
      }
    }
  }

  @Test
  public void testIncorrectUser() throws Exception {
    CloseableHttpClient httpclient = null;
    try {
      String username = "nouser";
      String password = "aaaa";
      httpclient = HttpClients.createDefault();

      HttpGet httpGet = new HttpGet("http://" + host + ":" + webUIPort);
      String authB64Code = B64Code.encode(username + ":" + password, StringUtil.__ISO_8859_1);
      httpGet.setHeader(HttpHeader.AUTHORIZATION.asString(), "Basic " + authB64Code);
      CloseableHttpResponse response = httpclient.execute(httpGet);
      Assert.assertTrue(response.toString().contains(Integer.toString(HttpURLConnection.HTTP_UNAUTHORIZED)));

    } finally {
      if (httpclient != null) {
        httpclient.close();
      }
    }
  }

  @Test
  public void testIncorrectPassword() throws Exception {
    CloseableHttpClient httpclient = null;
    try {
      String username = "user1";
      String password = "aaaa";
      httpclient = HttpClients.createDefault();

      HttpGet httpGet = new HttpGet("http://" + host + ":" + webUIPort);
      String authB64Code = B64Code.encode(username + ":" + password, StringUtil.__ISO_8859_1);
      httpGet.setHeader(HttpHeader.AUTHORIZATION.asString(), "Basic " + authB64Code);
      CloseableHttpResponse response = httpclient.execute(httpGet);
      Assert.assertTrue(response.toString().contains(Integer.toString(HttpURLConnection.HTTP_UNAUTHORIZED)));

    } finally {
      if (httpclient != null) {
        httpclient.close();
      }
    }
  }

  public static class TestPamAuthenticator extends PamAuthenticator {
    private static final Map<String, String> users = new HashMap<>();

    TestPamAuthenticator(HiveConf conf) throws AuthenticationException {
      super(conf);
    }

    static {
      users.put("user1", "1");
      users.put("user2", "2");
      users.put("user3", "3");
      users.put("user4", "4");
    }

    @Override protected UserIdentity login(String username, String password) {
      if (users.containsKey(username)) {
        if (users.get(username).equals(password)) {
          return new PamUserIdentity(username);
        }
      }
      return null;
    }
  }

  @AfterClass public static void afterTests() {
    hiveServer2.stop();
  }
}
