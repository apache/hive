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

package org.apache.hive.jdbc;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.http.HttpException;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.protocol.BasicHttpContext;
import org.junit.Assert;
import org.junit.Test;

public class TestHttpRequestInterceptor {

  @Test
  public void testRequestTrackHeader() throws HttpException, IOException {
    HttpRequestInterceptorBase requestInterceptor = getInterceptor();
    requestInterceptor.setRequestTrackingEnabled(true);
    requestInterceptor.sessionId(() -> "sessionId");
    requestInterceptor.process(new BasicHttpRequest("POST", "uri"), new BasicHttpContext());

    Assert.assertTrue(requestInterceptor.additionalHeaders.containsKey(Constants.HTTP_HEADER_REQUEST_TRACK));
    Assert.assertEquals("HIVE_sessionId_00000000000000000001",
        requestInterceptor.additionalHeaders.get(Constants.HTTP_HEADER_REQUEST_TRACK));
  }

  private HttpRequestInterceptorBase getInterceptor() {
    HttpRequestInterceptorBase requestInterceptor = new HttpBasicAuthInterceptor("user", "pass", new BasicCookieStore(),
        "cookieName", false, new HashMap<>(), new HashMap<>());
    return requestInterceptor;
  }
}
