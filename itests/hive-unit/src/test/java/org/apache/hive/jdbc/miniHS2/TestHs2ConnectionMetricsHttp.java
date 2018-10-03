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
package org.apache.hive.jdbc.miniHS2;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.HttpBasicAuthInterceptor;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.http.client.CookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the connection metrics using an HttpClient, when HS2 is start in http mode.
 */
public class TestHs2ConnectionMetricsHttp extends Hs2ConnectionMetrics {

  @BeforeClass
  public static void setup() throws Exception {
    confOverlay.clear();
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, "http");
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname, "cliservice");
    Hs2ConnectionMetrics.setup();
  }

  @AfterClass
  public static void tearDown() {
    Hs2ConnectionMetrics.tearDown();
  }

  @Test
  public void testOpenConnectionMetrics() throws Exception {
    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();

    TCLIService.Client httpClient = getHttpClient();
    TOpenSessionReq openSessionReq = new TOpenSessionReq();
    TOpenSessionResp tOpenSessionResp = httpClient.OpenSession(openSessionReq);
    // wait a couple of sec to make sure the connection is closed
    TimeUnit.SECONDS.sleep(3);
    verifyConnectionMetrics(metrics.dumpJson(), 0, 1);
    TSessionHandle sessionHandle = tOpenSessionResp.getSessionHandle();

    TCloseSessionReq closeSessionReq = new TCloseSessionReq(sessionHandle);
    httpClient.CloseSession(closeSessionReq);
    TimeUnit.SECONDS.sleep(3);
    verifyConnectionMetrics(metrics.dumpJson(), 0, 2);

    tOpenSessionResp = httpClient.OpenSession(openSessionReq);
    TimeUnit.SECONDS.sleep(3);
    verifyConnectionMetrics(metrics.dumpJson(), 0, 3);
    sessionHandle = tOpenSessionResp.getSessionHandle();

    closeSessionReq = new TCloseSessionReq(sessionHandle);
    httpClient.CloseSession(closeSessionReq);
    TimeUnit.SECONDS.sleep(3);
    verifyConnectionMetrics(metrics.dumpJson(), 0, 4);

  }

  private TCLIService.Client getHttpClient() throws Exception {
    DefaultHttpClient httpClient = new DefaultHttpClient();

    Map<String, String> headers = new HashMap<>();
    headers.put("Connection", "close");
    httpClient.addRequestInterceptor(new BasicHttpRequestInterceptor(USERNAME, PASSWORD, null,
            null, false, headers));

    TTransport transport = new THttpClient(getHttpUrl(), httpClient);
    TProtocol protocol = new TBinaryProtocol(transport);
    return new TCLIService.Client(protocol);
  }

  private String getHttpUrl() {
    return "http://" + miniHS2.getHost() + ":" + miniHS2.getHttpPort() + "/cliservice/";

  }

  private class BasicHttpRequestInterceptor extends HttpBasicAuthInterceptor {
    BasicHttpRequestInterceptor(String userName, String password, CookieStore cookieStore,
                                String cn, boolean isSSL,
                                Map<String, String> additionalHeaders) {
      super(userName, password, cookieStore, cn, isSSL, additionalHeaders, null);
    }
  }

}
