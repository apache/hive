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

package org.apache.hive.jdbc;

import java.util.Map;

import org.apache.http.HttpRequest;
import org.apache.http.client.CookieStore;
import org.apache.http.protocol.HttpContext;

/**
 * The class is instantiated with the username and password, it is then
 * used to add header with these credentials to HTTP requests
 *
 */
public class HttpTokenAuthInterceptor extends HttpRequestInterceptorBase {
  private String tokenStr;
  private static final String HIVE_DELEGATION_TOKEN_HEADER =  "X-Hive-Delegation-Token";
  
  public HttpTokenAuthInterceptor(String tokenStr, CookieStore cookieStore, String cn,
      boolean isSSL, Map<String, String> additionalHeaders) {
    super(cookieStore, cn, isSSL, additionalHeaders);
    this.tokenStr = tokenStr;
  }

  @Override
  protected void addHttpAuthHeader(HttpRequest httpRequest, HttpContext httpContext)
    throws Exception {
    httpRequest.addHeader(HIVE_DELEGATION_TOKEN_HEADER, tokenStr);
  }
}