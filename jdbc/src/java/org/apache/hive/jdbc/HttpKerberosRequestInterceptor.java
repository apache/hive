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

  // A fair reentrant lock
  private static ReentrantLock kerberosLock = new ReentrantLock(true);

  public HttpKerberosRequestInterceptor(String principal, String host,
      String serverHttpUrl) {
    this.principal = principal;
    this.host = host;
    this.serverHttpUrl = serverHttpUrl;
  }

  @Override
  public void process(HttpRequest httpRequest, HttpContext httpContext)
      throws HttpException, IOException {
    String kerberosAuthHeader;
    try {
      // Generate the service ticket for sending to the server.
      // Locking ensures the tokens are unique in case of concurrent requests
      kerberosLock.lock();
      kerberosAuthHeader = HttpAuthUtils.getKerberosServiceTicket(
          principal, host, serverHttpUrl);
      // Set the session key token (Base64 encoded) in the headers
      httpRequest.addHeader(HttpAuthUtils.AUTHORIZATION + ": " +
          HttpAuthUtils.NEGOTIATE + " ", kerberosAuthHeader);
    } catch (Exception e) {
      throw new HttpException(e.getMessage(), e);
    }
    finally {
      kerberosLock.unlock();
    }
  }
}
