/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.services.impl;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import com.google.common.collect.ImmutableSet;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertNotNull;

public class TestLlapWebServices {

  private static LlapWebServices llapWS = null;
  private static int llapWSPort;

  @BeforeClass
  public static void beforeTests() throws Exception {
    llapWSPort = MetaStoreTestUtils.findFreePortExcepting(
        Integer.valueOf(HiveConf.ConfVars.LLAP_DAEMON_WEB_PORT.getDefaultValue()));
    llapWS = new LlapWebServices(llapWSPort, null, null);
    llapWS.init(new HiveConf());
    llapWS.start();
    Thread.sleep(5000);
    ensureUniqueInClasspath("javax/servlet/http/HttpServletRequest.class");
    ensureUniqueInClasspath("javax/servlet/http/HttpServlet.class");
  }

  private static void ensureUniqueInClasspath(String name) throws IOException {
    Enumeration<URL> rr = TestLlapWebServices.class.getClassLoader().getResources(name);
    List<URL> found = new ArrayList<>();
    while (rr.hasMoreElements()) {
      found.add(rr.nextElement());
    }
    if (found.size() != 1) {
      throw new RuntimeException(name + " unexpected number of occurences on the classpath:" + found.toString());
    }
  }

  @Test
  public void testContextRootUrlRewrite() throws Exception {
    String contextRootURL = "http://localhost:" + llapWSPort + "/";
    String contextRootContent = getURLResponseAsString(contextRootURL, HTTP_OK);

    String indexHtmlUrl = "http://localhost:" + llapWSPort + "/index.html";
    String indexHtmlContent = getURLResponseAsString(indexHtmlUrl, HTTP_OK);

    Assert.assertEquals(contextRootContent, indexHtmlContent);
  }

  @Test
  public void testDirListingDisabled() throws Exception {
    for (String folder : ImmutableSet.of("images", "js", "css")) {
      String url = "http://localhost:" + llapWSPort + "/" + folder;
      getURLResponseAsString(url, HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testBaseUrlResponseHeader() throws Exception{
    String baseURL = "http://localhost:" + llapWSPort + "/";
    URL url = new URL(baseURL);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    String xfoHeader = conn.getHeaderField("X-FRAME-OPTIONS");
    String xXSSProtectionHeader = conn.getHeaderField("X-XSS-Protection");
    String xContentTypeHeader = conn.getHeaderField("X-Content-Type-Options");
    assertNotNull(xfoHeader);
    assertNotNull(xXSSProtectionHeader);
    assertNotNull(xContentTypeHeader);
  }

  private static String getURLResponseAsString(String baseURL, int expectedStatus)
      throws IOException {
    URL url = new URL(baseURL);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(expectedStatus, conn.getResponseCode());
    if (expectedStatus != HTTP_OK) {
      return null;
    }
    StringWriter writer = new StringWriter();
    IOUtils.copy(conn.getInputStream(), writer, "UTF-8");
    return writer.toString();
  }

  @AfterClass
  public static void afterTests() throws Exception {
    llapWS.stop();
  }
}
