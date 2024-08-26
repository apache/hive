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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.util.Time;
import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.client.CookieStore;
import org.apache.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HttpRequestInterceptorBase implements HttpRequestInterceptor {
  protected final Logger LOG = LoggerFactory.getLogger(getClass());

  CookieStore cookieStore;
  boolean isCookieEnabled;
  String cookieName;
  boolean isSSL;
  Map<String, String> additionalHeaders;
  Map<String, String> customCookies;
  private Supplier<String> sessionId = null;
  private boolean requestTrackingEnabled;
  private final AtomicLong requestTrackCounter = new AtomicLong();

  // Abstract function to add HttpAuth Header
  protected abstract void addHttpAuthHeader(HttpRequest httpRequest, HttpContext httpContext)
    throws Exception;

  public HttpRequestInterceptorBase(CookieStore cs, String cn, boolean isSSL,
      Map<String, String> additionalHeaders, Map<String, String> customCookies) {
    this.cookieStore = cs;
    this.isCookieEnabled = (cs != null);
    this.cookieName = cn;
    this.isSSL = isSSL;
    this.additionalHeaders = additionalHeaders == null ? new HashMap<>() : additionalHeaders;
    this.customCookies = customCookies;
  }

  @Override
  public void process(HttpRequest httpRequest, HttpContext httpContext)
    throws HttpException, IOException {
    try {
      // If cookie based authentication is allowed, generate ticket only when necessary.
      // The necessary condition is either when there are no server side cookies in the
      // cookiestore which can be send back or when the server returns a 401 error code
      // indicating that the previous cookie has expired.

      // Generate the kerberos ticket under the following scenarios:
      // 1. Cookie Authentication is disabled OR
      // 2. The first time when the request is sent OR
      // 3. The server returns a 401, which sometimes means the cookie has expired
      // 4. The cookie is secured where as the client connect does not use SSL
      if (!isCookieEnabled || ((httpContext.getAttribute(Utils.HIVE_SERVER2_RETRY_KEY) == null &&
          (cookieStore == null || (cookieStore != null &&
          Utils.needToSendCredentials(cookieStore, cookieName, isSSL)))) ||
          (httpContext.getAttribute(Utils.HIVE_SERVER2_RETRY_KEY) != null &&
          httpContext.getAttribute(Utils.HIVE_SERVER2_RETRY_KEY).
          equals(Utils.HIVE_SERVER2_CONST_TRUE)))) {
        addHttpAuthHeader(httpRequest, httpContext);
        httpContext.setAttribute(Utils.HIVE_SERVER2_SENT_CREDENTIALS, Utils.HIVE_SERVER2_CONST_TRUE);
      } else {
        httpContext.setAttribute(Utils.HIVE_SERVER2_SENT_CREDENTIALS, Utils.HIVE_SERVER2_CONST_FALSE);
      }
      if (isCookieEnabled) {
        httpContext.setAttribute(Utils.HIVE_SERVER2_RETRY_KEY, Utils.HIVE_SERVER2_CONST_FALSE);
      }

      if (requestTrackingEnabled) {
        String trackHeader = getNewTrackHeader();
        LOG.info("{}:{}", Constants.HTTP_HEADER_REQUEST_TRACK, trackHeader);
        additionalHeaders.put(Constants.HTTP_HEADER_REQUEST_TRACK, trackHeader);
        httpContext.setAttribute(Constants.HTTP_HEADER_REQUEST_TRACK, trackHeader);
        httpContext.setAttribute(trackHeader + Constants.TIME_POSTFIX_REQUEST_TRACK, Time.monotonicNow());
      }
      // Insert the additional http headers
      if (additionalHeaders != null) {
        for (Map.Entry<String, String> entry : additionalHeaders.entrySet()) {
          httpRequest.addHeader(entry.getKey(), entry.getValue());
        }
      }
      // Add custom cookies if passed to the jdbc driver
      if (customCookies != null && !customCookies.isEmpty()) {
        String cookieHeaderKeyValues = "";
        Header cookieHeaderServer = httpRequest.getFirstHeader("Cookie");
        if ((cookieHeaderServer != null) && (cookieHeaderServer.getValue() != null)) {
          cookieHeaderKeyValues = cookieHeaderServer.getValue();
        }
        for (Map.Entry<String, String> entry : customCookies.entrySet()) {
          cookieHeaderKeyValues += ";" + entry.getKey() + "=" + entry.getValue();
        }
        if (cookieHeaderKeyValues.startsWith(";")) {
          cookieHeaderKeyValues = cookieHeaderKeyValues.substring(1);
        }
        httpRequest.addHeader("Cookie", cookieHeaderKeyValues);
      }
    } catch (Exception e) {
      throw new HttpException(e.getMessage(), e);
    }
  }

  protected String getNewTrackHeader() {
    return String.format("HIVE_%s_%020d", sessionId.get(), requestTrackCounter.incrementAndGet());
  }

  public HttpRequestInterceptor sessionId(Supplier<String> sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public void setRequestTrackingEnabled(boolean requestTrackingEnabled) {
    this.requestTrackingEnabled = requestTrackingEnabled;
  }
}
