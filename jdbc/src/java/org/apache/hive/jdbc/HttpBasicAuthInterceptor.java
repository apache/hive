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

  public HttpBasicAuthInterceptor(String username, String password) {
    if(username != null){
      credentials = new UsernamePasswordCredentials(username, password);
    }
    authScheme = new BasicScheme();
  }

  @Override
  public void process(HttpRequest httpRequest, HttpContext httpContext)
      throws HttpException, IOException {
    Header basicAuthHeader = authScheme.authenticate(
        credentials, httpRequest, httpContext);
    httpRequest.addHeader(basicAuthHeader);
  }

}
