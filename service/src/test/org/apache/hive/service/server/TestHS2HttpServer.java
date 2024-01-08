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

package org.apache.hive.service.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * TestHS2HttpServer -- executes tests of HiveServer2 HTTP Server.
 */
public class TestHS2HttpServer {

  private static HiveServer2 hiveServer2 = null;
  private static CLIService client = null;
  private static SessionManager sm = null;
  private static HiveConf hiveConf = null;
  private static String metastorePasswd = "61ecbc41cdae3e6b32712a06c73606fa"; //random md5
  private static Integer webUIPort = null;
  private static String apiBaseURL = null;


  @BeforeClass
  public static void beforeTests() throws Exception {
    webUIPort = MetaStoreTestUtils.findFreePortExcepting(
        Integer.valueOf(ConfVars.HIVE_SERVER2_WEBUI_PORT.getDefaultValue()));
    apiBaseURL = "http://localhost:" + webUIPort + "/api/v1";
    hiveConf = new HiveConf();
    hiveConf.set(ConfVars.METASTORE_PWD.varname, metastorePasswd);
    hiveConf.set(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname, webUIPort.toString());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");

    Exception hs2Exception = null;
    boolean hs2Started = false;
    for (int tryCount = 0; (tryCount < MetaStoreTestUtils.RETRY_COUNT); tryCount++) {
      try {
        hiveServer2 = new HiveServer2();
        hiveServer2.init(hiveConf);
        hiveServer2.start();
        client = hiveServer2.getCliService();
        Thread.sleep(5000);
        hs2Started = true;
        break;
      } catch (Exception t) {
        HiveConf.setIntVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT,
            MetaStoreTestUtils.findFreePort());
        HiveConf.setIntVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT,
            MetaStoreTestUtils.findFreePort());
        HiveConf.setIntVar(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT,
            MetaStoreTestUtils.findFreePort());
        webUIPort = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT);
      }
    }
    if (!hs2Started) {
      throw (hs2Exception);
    }
    sm = hiveServer2.getCliService().getSessionManager();
  }

  @Test
  public void testStackServlet() throws Exception {
    String baseURL = "http://localhost:" + webUIPort + "/stacks";
    URL url = new URL(baseURL);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HTTP_OK, conn.getResponseCode());
    BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream()));
    boolean contents = false;
    String line;
    while ((line = reader.readLine()) != null) {
      if (line.contains("Process Thread Dump:")) {
        contents = true;
      }
    }
    Assert.assertTrue(contents);
  }

  @Test
  public void testBaseUrlResponseHeader() throws Exception{
    String baseURL = "http://localhost:" + webUIPort + "/";
    URL url = new URL(baseURL);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    String xXSSProtectionHeader = conn.getHeaderField("X-XSS-Protection");
    String xContentTypeHeader = conn.getHeaderField("X-Content-Type-Options");
    assertNotNull(xfoHeader);
    assertNotNull(xXSSProtectionHeader);
    assertNotNull(xContentTypeHeader);
  }

  @Test
  public void testDirListingDisabledOnStaticServlet() throws Exception {
    String url = "http://localhost:" + webUIPort + "/static";
    getReaderForUrl(url, HTTP_FORBIDDEN);
  }

  private BufferedReader getReaderForUrl(String urlString, int expectedStatus) throws Exception {
    URL url = new URL(urlString);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(expectedStatus, conn.getResponseCode());
    if (expectedStatus != HTTP_OK) {
      return null;
    }

    BufferedReader reader =
        new BufferedReader(new InputStreamReader(conn.getInputStream()));
    return reader;
  }

  private String readFromUrl(String urlString) throws Exception {
    BufferedReader reader = getReaderForUrl(urlString, HTTP_OK);
    StringBuilder response = new StringBuilder();
    String inputLine;

    while ((inputLine = reader.readLine()) != null) {
      response.append(inputLine);
    }
    reader.close();
    return response.toString();
  }

  private static List<JsonNode> getListOfNodes(String json) throws Exception {
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(json);

    ArrayList<JsonNode> nodes = new ArrayList<>();
    if (rootNode.isArray()) {
      for (final JsonNode node : rootNode) {
        nodes.add(node);
      }
    }
    return nodes;
  }

  @Test
  public void testApiServletHistoricalQueries() throws Exception {
    String historicalQueriesRoute = "/queries/historical";

    final SessionHandle handle =
        sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
            new HashMap());

    String queryString = "SET " + HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname
        + " = false";
    OperationHandle opHandle = client.executeStatement(handle, queryString, new HashMap());
    client.closeOperation(opHandle);

    opHandle = client.executeStatement(handle, "SELECT 1", new HashMap());
    client.closeOperation(opHandle);

    String queriesResponse = readFromUrl(apiBaseURL + historicalQueriesRoute);
    List<JsonNode> historicalQueries = getListOfNodes(queriesResponse);
    Assert.assertTrue(historicalQueries.size() == 1);

    JsonNode historicalQuery = historicalQueries.get(0);
    Assert.assertEquals(historicalQuery.path("running").asBoolean(), false);
    Assert.assertEquals(historicalQuery.path("state").asText(), "FINISHED");
    Assert.assertTrue(historicalQuery.path("runtime").canConvertToInt());
    Assert.assertTrue(historicalQuery.path("queryDisplay").isObject());
  }

  @Test
  public void testApiServletActiveSessions() throws Exception {
    String sessionsRoute = "/sessions";

    String initNoSessionsResponse = readFromUrl(apiBaseURL + sessionsRoute);
    Assert.assertTrue("[]".equals(initNoSessionsResponse));

    SessionHandle handle1 =
        sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
            new HashMap());

    String oneSessionResponse = readFromUrl(apiBaseURL + sessionsRoute);

    List<JsonNode> sessionNodes = getListOfNodes(oneSessionResponse);
    Assert.assertEquals(sessionNodes.size(), 1);

    JsonNode session = sessionNodes.get(0);
    Assert.assertEquals(session.path("sessionId").asText(), handle1.getSessionId().toString());
    Assert.assertEquals(session.path("username").asText(), "user");
    Assert.assertEquals(session.path("ipAddress").asText(), "127.0.0.1");
    Assert.assertEquals(session.path("operationCount").asInt(), 0);
    Assert.assertTrue(session.path("activeTime").canConvertToInt());
    Assert.assertTrue(session.path("idleTime").canConvertToInt());

    SessionHandle handle2 = sm.openSession(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9, "user", "passw", "127.0.0.1",
        new HashMap());

    String twoSessionsResponse = readFromUrl(apiBaseURL + sessionsRoute);
    List<JsonNode> twoSessionsNodes = getListOfNodes(twoSessionsResponse);
    Assert.assertEquals(twoSessionsNodes.size(), 2);

    sm.closeSession(handle1);
    sm.closeSession(handle2);

    String endNoSessionsResponse = readFromUrl(apiBaseURL + sessionsRoute);
    Assert.assertTrue("[]".equals(endNoSessionsResponse));
  }

  @Test
  public void testWrongApiVersion() throws Exception {
    String wrongApiVersionUrl = "http://localhost:" + webUIPort + "/api/v2";
    URL url = new URL(wrongApiVersionUrl);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
  }

  @Test
  public void testWrongRoute() throws Exception {
    String wrongRouteUrl = "http://localhost:" + webUIPort + "/api/v1/nonexistingRoute";
    URL url = new URL(wrongRouteUrl);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_NOT_FOUND, conn.getResponseCode());
  }

  @Test
  public void testContextRootUrlRewrite() throws Exception {
    String datePattern = "[a-zA-Z]{3} [a-zA-Z]{3} [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}";
    String dateMask = "xxxMasked_DateTime_xxx";
    String baseURL = "http://localhost:" + webUIPort + "/";
    String contextRootContent = getURLResponseAsString(baseURL);

    String jspUrl = "http://localhost:" + webUIPort + "/hiveserver2.jsp";
    String jspContent = getURLResponseAsString(jspUrl);

    String expected = contextRootContent.replaceAll(datePattern, dateMask);
    String actual = jspContent.replaceAll(datePattern, dateMask);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testConfStrippedFromWebUI() throws Exception {

    String pwdValFound = null;
    String pwdKeyFound = null;
    CloseableHttpClient httpclient = null;
    try {
      httpclient = HttpClients.createDefault();
      HttpGet httpGet = new HttpGet("http://localhost:" + webUIPort + "/conf");
      CloseableHttpResponse response1 = httpclient.execute(httpGet);

      try {
        HttpEntity entity1 = response1.getEntity();
        BufferedReader br = new BufferedReader(new InputStreamReader(entity1.getContent()));
        String line;
        while ((line = br.readLine()) != null) {
          if (line.contains(metastorePasswd)) {
            pwdValFound = line;
          }
          if (line.contains(ConfVars.METASTORE_PWD.varname)) {
            pwdKeyFound = line;
          }
        }
        EntityUtils.consume(entity1);
      } finally {
        response1.close();
      }
    } finally {
      if (httpclient != null) {
        httpclient.close();
      }
    }

    assertNotNull(pwdKeyFound);
    assertNull(pwdValFound);
  }

  private String getURLResponseAsString(String baseURL) throws IOException {
    URL url = new URL(baseURL);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals("Got an HTTP response code other thank OK.", HTTP_OK, conn.getResponseCode());
    StringWriter writer = new StringWriter();
    IOUtils.copy(conn.getInputStream(), writer, "UTF-8");
    return writer.toString();
  }


  @AfterClass
  public static void afterTests() throws Exception {
    hiveServer2.stop();
  }
}
