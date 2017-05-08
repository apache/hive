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

package org.apache.hive.service.server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * TestHS2HttpServer -- executes tests of HiveServer2 HTTP Server
 */
public class TestHS2HttpServer {

  private static HiveServer2 hiveServer2 = null;
  private static HiveConf hiveConf = null;
  private static String metastorePasswd = "61ecbc41cdae3e6b32712a06c73606fa"; //random md5
  private static Integer webUIPort = null;

  @BeforeClass
  public static void beforeTests() throws Exception {
    webUIPort = MetaStoreUtils.findFreePortExcepting(
        Integer.valueOf(ConfVars.HIVE_SERVER2_WEBUI_PORT.getDefaultValue()));
    hiveConf = new HiveConf();
    hiveConf.set(ConfVars.METASTOREPWD.varname, metastorePasswd);
    hiveConf.set(ConfVars.HIVE_SERVER2_WEBUI_PORT.varname, webUIPort.toString());
    hiveConf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    Thread.sleep(5000);
  }

  @Test
  public void testStackServlet() throws Exception {
    String baseURL = "http://localhost:" + webUIPort + "/stacks";
    URL url = new URL(baseURL);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
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
  public void testContextRootUrlRewrite() throws Exception {
    String datePattern = "[a-zA-Z]{3} [a-zA-Z]{3} [0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]+\\[[0-9]+]";
    String dateMask = "xxxMasked_DateTime_xxx";
    String baseURL = "http://localhost:" + webUIPort + "/";
    String contextRootContent = getURLResponseAsString(baseURL);

    String jspUrl = "http://localhost:" + webUIPort + "/hiveserver2.jsp";
    String jspContent = getURLResponseAsString(jspUrl);

    Assert.assertEquals(contextRootContent.replaceAll(datePattern, dateMask),
                        jspContent.replaceAll(datePattern, dateMask));
  }

  @Test
  public void testConfStrippedFromWebUI() throws Exception {

    String pwdValFound = null;
    String pwdKeyFound = null;
    CloseableHttpClient httpclient = null;
    try {
      httpclient = HttpClients.createDefault();
      HttpGet httpGet = new HttpGet("http://localhost:"+webUIPort+"/conf");
      CloseableHttpResponse response1 = httpclient.execute(httpGet);

      try {
        HttpEntity entity1 = response1.getEntity();
        BufferedReader br = new BufferedReader(new InputStreamReader(entity1.getContent()));
        String line;
        while ((line = br.readLine())!= null) {
          if (line.contains(metastorePasswd)){
            pwdValFound = line;
          }
          if (line.contains(ConfVars.METASTOREPWD.varname)){
            pwdKeyFound = line;
          }
        }
        EntityUtils.consume(entity1);
      } finally {
        response1.close();
      }
    } finally {
      if (httpclient != null){
        httpclient.close();
      }
    }

    assertNotNull(pwdKeyFound);
    assertNull(pwdValFound);
  }

  private String getURLResponseAsString(String baseURL) throws IOException {
    URL url = new URL(baseURL);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    StringWriter writer = new StringWriter();
    IOUtils.copy(conn.getInputStream(), writer, "UTF-8");
    return writer.toString();
  }


  @AfterClass
  public static void afterTests() throws Exception {
    hiveServer2.stop();
  }
}
