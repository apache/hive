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
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hive.service.auth.HttpAuthUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.CookieStore;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.protocol.HttpContext;

/**
 *
 * Authentication interceptor which adds Base64 encoded payload,
 * containing the username and kerberos service ticket,
 * to the outgoing http request header.
 *
 */
public class HttpKerberosRequestInterceptor implements HttpRequestInterceptor {

  String principal;
  String host;
  String serverHttpUrl;
  boolean assumeSubject;
  CookieStore cookieStore;
  boolean isCookieEnabled;
  String cookieName;

  // A fair reentrant lock
  private static ReentrantLock kerberosLock = new ReentrantLock(true);

  public HttpKerberosRequestInterceptor(String principal, String host,
      String serverHttpUrl, boolean assumeSubject, CookieStore cs, String cn) {
    this.principal = principal;
    this.host = host;
    this.serverHttpUrl = serverHttpUrl;
    this.assumeSubject = assumeSubject;
    this.cookieStore = cs;
    isCookieEnabled = (cs != null);
    cookieName = cn;
  }

  @Override
  public void process(HttpRequest httpRequest, HttpContext httpContext)
      throws HttpException, IOException {
    String kerberosAuthHeader;

    try {
      // Generate the service ticket for sending to the server.
      // Locking ensures the tokens are unique in case of concurrent requests
      kerberosLock.lock();
      // If cookie based authentication is allowed, generate ticket only when necessary.
      // The necessary condition is either when there are no server side cookies in the
      // cookiestore which can be send back or when the server returns a 401 error code
      // indicating that the previous cookie has expired.
      if (isCookieEnabled) {
        httpContext.setAttribute(ClientContext.COOKIE_STORE, cookieStore);
      }
      // Generate the kerberos ticket under the following scenarios:
      // 1. Cookie Authentication is disabled OR
      // 2. The first time when the request is sent OR
      // 3. The server returns a 401, which sometimes means the cookie has expired
      if (!isCookieEnabled || ((httpContext.getAttribute(Utils.HIVE_SERVER2_RETRY_KEY) == null &&
          (cookieStore == null || (cookieStore != null &&
          Utils.needToSendCredentials(cookieStore, cookieName)))) ||
          (httpContext.getAttribute(Utils.HIVE_SERVER2_RETRY_KEY) != null &&
          httpContext.getAttribute(Utils.HIVE_SERVER2_RETRY_KEY).
          equals(Utils.HIVE_SERVER2_RETRY_TRUE)))) {
        kerberosAuthHeader = HttpAuthUtils.getKerberosServiceTicket(
            principal, host, serverHttpUrl, assumeSubject);
        // Set the session key token (Base64 encoded) in the headers
        httpRequest.addHeader(HttpAuthUtils.AUTHORIZATION + ": " +
            HttpAuthUtils.NEGOTIATE + " ", kerberosAuthHeader);
      }
      if (isCookieEnabled) {
        httpContext.setAttribute(Utils.HIVE_SERVER2_RETRY_KEY, Utils.HIVE_SERVER2_RETRY_FALSE);
      }
    } catch (Exception e) {
      throw new HttpException(e.getMessage(), e);
    }
    finally {
      kerberosLock.unlock();
    }
  }
}
