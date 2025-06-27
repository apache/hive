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

package org.apache.hive.service;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.Header;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

public class TestHttpServices {

  private static MiniHS2 miniHS2 = null;

  @BeforeClass
  public static void startServices() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(MetastoreConf.ConfVars.THRIFT_TRANSPORT_MODE.toString(), "http"); // HS2 -> HMS thrift on http

    miniHS2 = new MiniHS2.Builder()
            .withConf(hiveConf)
            .withHTTPTransport() // Cli service -> HS2 thrift on http
            .withRemoteMetastore()
            .build();

    miniHS2.start(new HashMap<>());
  }

  @AfterClass
  public static void stopServices() {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testWebUIResponseDoesNotContainServerVersionAndXPoweredBy() throws Exception {
    testHttpServiceDoesNotContainServerVersionAndXPoweredBy(
            "http://" + miniHS2.getHost() + ":" + miniHS2.getWebPort());
  }

  @Test
  public void testCliServiceResponseDoesNotContainServerVersionAndXPoweredBy() throws Exception {
    testHttpServiceDoesNotContainServerVersionAndXPoweredBy(
            "http://" + miniHS2.getHost() + ":" + miniHS2.getWebPort() + "/cliservice");
  }

  @Test
  public void testHMSServiceResponseDoesNotContainServerVersionAndXPoweredBy() throws Exception {
    testHttpServiceDoesNotContainServerVersionAndXPoweredBy(
            "http://" + miniHS2.getHost() + ":" + miniHS2.getWebPort() + "/" +
            MetastoreConf.ConfVars.METASTORE_CLIENT_THRIFT_HTTP_PATH.getDefaultVal());
  }

  private void testHttpServiceDoesNotContainServerVersionAndXPoweredBy(String miniHS2) throws IOException {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpGet request = new HttpGet(miniHS2);

      try (CloseableHttpResponse response = httpClient.execute(request)) {
        for (Header header : response.getAllHeaders()) {
          Assert.assertNotEquals("x-powered-by", header.getName().toLowerCase());
          Assert.assertNotEquals("server", header.getName().toLowerCase());
        }
      }
    }
  }
}
