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
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.URL;

public class TestLlapWebServices {

  private static LlapWebServices llapWS = null;
  private static int llapWSPort;

  @BeforeClass
  public static void beforeTests() throws Exception {
    llapWSPort = MetaStoreUtils.findFreePortExcepting(
        Integer.valueOf(HiveConf.ConfVars.LLAP_DAEMON_WEB_PORT.getDefaultValue()));
    llapWS = new LlapWebServices(llapWSPort, null, null);
    llapWS.init(new HiveConf());
    llapWS.start();
    Thread.sleep(5000);
  }

  @Test
  public void testContextRootUrlRewrite() throws Exception {
    String contextRootURL = "http://localhost:" + llapWSPort + "/";
    String contextRootContent = getURLResponseAsString(contextRootURL);

    String indexHtmlUrl = "http://localhost:" + llapWSPort + "/index.html";
    String indexHtmlContent = getURLResponseAsString(indexHtmlUrl);

    Assert.assertEquals(contextRootContent, indexHtmlContent);
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
    llapWS.stop();
  }
}
