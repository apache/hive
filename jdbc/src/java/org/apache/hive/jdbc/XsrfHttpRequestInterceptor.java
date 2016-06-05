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

import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;

public class XsrfHttpRequestInterceptor implements HttpRequestInterceptor {

  // Note : This implements HttpRequestInterceptor rather than extending
  // HttpRequestInterceptorBase, because that class is an auth-specific
  // class and refactoring would kludge too many things that are potentially
  // public api.
  //
  // At the base, though, what we do is a very simple thing to protect
  // against CSRF attacks, and that is to simply add another header. If
  // HS2 is running with an XSRF filter enabled, then it will reject all
  // requests that do not contain this. Thus, we add this in here on the
  // client-side. This simple check prevents random other websites from
  // redirecting a browser that has login credentials from making a
  // request to HS2 on their behalf.

  private static boolean injectHeader = true;

  public static void enableHeaderInjection(boolean enabled){
    injectHeader = enabled;
  }

  @Override
  public void process(HttpRequest httpRequest, HttpContext httpContext)
      throws HttpException, IOException {
    if (injectHeader){
      httpRequest.addHeader("X-XSRF-HEADER", "true");
    }
  }
}
