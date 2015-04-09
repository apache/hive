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

import java.io.IOException;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CookieStore;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.auth.AuthSchemeBase;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.protocol.HttpContext;

/**
 * The class is instantiated with the username and password, it is then
 * used to add header with these credentials to HTTP requests
 *
 */
public class HttpBasicAuthInterceptor implements HttpRequestInterceptor {
  UsernamePasswordCredentials credentials;
  AuthSchemeBase authScheme;
  CookieStore cookieStore;
  boolean isCookieEnabled;
  String cookieName;

  public HttpBasicAuthInterceptor(String username, String password, CookieStore cookieStore,
                           String cn) {
    if(username != null){
      credentials = new UsernamePasswordCredentials(username, password);
    }
    authScheme = new BasicScheme();
    this.cookieStore = cookieStore;
    isCookieEnabled = (cookieStore != null);
    cookieName = cn;
  }

  @Override
  public void process(HttpRequest httpRequest, HttpContext httpContext)
      throws HttpException, IOException {
    if (isCookieEnabled) {
      httpContext.setAttribute(ClientContext.COOKIE_STORE, cookieStore);
    }
    // Add the authentication details under the following scenarios:
    // 1. Cookie Authentication is disabled OR
    // 2. The first time when the request is sent OR
    // 3. The server returns a 401, which sometimes means the cookie has expired
    if (!isCookieEnabled || ((httpContext.getAttribute(Utils.HIVE_SERVER2_RETRY_KEY) == null &&
        (cookieStore == null || (cookieStore != null &&
        Utils.needToSendCredentials(cookieStore, cookieName)))) ||
        (httpContext.getAttribute(Utils.HIVE_SERVER2_RETRY_KEY) != null &&
         httpContext.getAttribute(Utils.HIVE_SERVER2_RETRY_KEY).
         equals(Utils.HIVE_SERVER2_RETRY_TRUE)))) {
      Header basicAuthHeader = authScheme.authenticate(credentials, httpRequest, httpContext);
      httpRequest.addHeader(basicAuthHeader);
    }
    if (isCookieEnabled) {
      httpContext.setAttribute(Utils.HIVE_SERVER2_RETRY_KEY, Utils.HIVE_SERVER2_RETRY_FALSE);
    }
  }
}
