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

package org.apache.hadoop.hive.druid.http;

import org.apache.hadoop.hive.druid.security.DruidKerberosUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.CookieManager;
import java.net.HttpCookie;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Apache HttpClient based HTTP client for Druid REST calls.
 * Replaces Druid's Netty 3 HttpClientInit/KerberosHttpClient stack (HIVE-25013).
 */
public class HiveDruidHttpClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveDruidHttpClient.class);

  private final CloseableHttpClient httpClient;
  private final CookieManager cookieManager;
  private final boolean kerberosEnabled;
  private final ExecutorService executor;

  public HiveDruidHttpClient(int readTimeoutMs, boolean kerberosEnabled) {
    this.kerberosEnabled = kerberosEnabled && UserGroupInformation.isSecurityEnabled();
    this.cookieManager = new CookieManager();
    this.httpClient = HttpClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom()
            .setConnectTimeout(readTimeoutMs)
            .setSocketTimeout(readTimeoutMs)
            .setConnectionRequestTimeout(readTimeoutMs)
            .build())
        .disableCookieManagement()
        .build();
    this.executor = Executors.newCachedThreadPool(r -> {
      Thread t = new Thread(r, "HiveDruidHttpClient");
      t.setDaemon(true);
      return t;
    });
    if (this.kerberosEnabled) {
      LOG.info("HiveDruidHttpClient Kerberos authentication enabled");
    }
  }

  public HiveDruidHttpResponse execute(HiveDruidHttpRequest request) throws IOException {
    return innerExecute(request);
  }

  public InputStream executeStream(HiveDruidHttpRequest request) throws IOException {
    HiveDruidHttpResponse response = innerExecute(request);
    return new java.io.ByteArrayInputStream(response.getBody());
  }

  public Future<InputStream> executeStreamAsync(HiveDruidHttpRequest request) {
    return executor.submit(() -> executeStream(request));
  }

  private HiveDruidHttpResponse innerExecute(HiveDruidHttpRequest request) throws IOException {
    HiveDruidHttpRequest current = request.copy();
    boolean shouldRetryOnUnauthorized = prepareKerberosAuth(current);

    while (true) {
      try (CloseableHttpResponse response = httpClient.execute(buildHttpRequest(current))) {
        int statusCode = response.getStatusLine().getStatusCode();
        storeCookies(current.getUrl().toURI(), response);
        byte[] body = EntityUtils.toByteArray(response.getEntity());

        if (kerberosEnabled && shouldRetryOnUnauthorized
            && statusCode == HiveDruidHttpResponse.SC_UNAUTHORIZED) {
          LOG.debug("Received 401 for URI {}, retrying with fresh Kerberos credentials", current.getUrl());
          DruidKerberosUtil.removeAuthCookie(cookieManager.getCookieStore(), current.getUrl().toURI());
          current = request.copy();
          current.setHeader("Cookie", "");
          shouldRetryOnUnauthorized = prepareKerberosAuth(current);
          continue;
        }

        return new HiveDruidHttpResponse(statusCode, body, extractHeaders(response));
      } catch (IOException e) {
        throw e;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  private boolean prepareKerberosAuth(HiveDruidHttpRequest request) throws IOException {
    if (!kerberosEnabled) {
      return false;
    }
    try {
      URI uri = request.getUrl().toURI();
      Map<String, List<String>> cookieHeaders = cookieManager.get(uri, Collections.emptyMap());
      for (Map.Entry<String, List<String>> entry : cookieHeaders.entrySet()) {
        request.addHeaderValues(entry.getKey(), entry.getValue());
      }

      if (DruidKerberosUtil.needToSendCredentials(cookieManager.getCookieStore(), uri)) {
        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
        currentUser.checkTGTAndReloginFromKeytab();
        String challenge = currentUser.doAs((PrivilegedExceptionAction<String>) () ->
            DruidKerberosUtil.kerberosChallenge(request.getUrl().getHost()));
        request.setHeader("Authorization", "Negotiate " + challenge);
        return false;
      }
      return true;
    } catch (Exception e) {
      throw new IOException("Failed to prepare Kerberos authentication", e);
    }
  }

  private static HttpUriRequest buildHttpRequest(HiveDruidHttpRequest request) throws IOException {
    HttpRequestBase httpRequest;
    if ("POST".equalsIgnoreCase(request.getMethod())) {
      HttpPost post = new HttpPost(request.getUrl().toString());
      if (request.hasContent()) {
        post.setEntity(new ByteArrayEntity(request.getContent()));
      }
      httpRequest = post;
    } else if ("GET".equalsIgnoreCase(request.getMethod())) {
      httpRequest = new HttpGet(request.getUrl().toString());
    } else {
      HttpEntityEnclosingRequestBase custom = new HttpEntityEnclosingRequestBase() {
        @Override
        public String getMethod() {
          return request.getMethod();
        }
      };
      custom.setURI(URI.create(request.getUrl().toString()));
      if (request.hasContent()) {
        custom.setEntity(new ByteArrayEntity(request.getContent()));
      }
      httpRequest = custom;
    }
    for (Map.Entry<String, List<String>> header : request.getHeaders().entrySet()) {
      for (String value : header.getValue()) {
        httpRequest.addHeader(header.getKey(), value);
      }
    }
    return httpRequest;
  }

  private static Map<String, List<String>> extractHeaders(CloseableHttpResponse response) {
    Map<String, List<String>> headers = new HashMap<>();
    for (org.apache.http.Header header : response.getAllHeaders()) {
      headers.computeIfAbsent(header.getName(), k -> new ArrayList<>()).add(header.getValue());
    }
    return headers;
  }

  private void storeCookies(URI uri, CloseableHttpResponse response) {
    Map<String, List<String>> headerMap = new HashMap<>();
    for (org.apache.http.Header header : response.getHeaders("Set-Cookie")) {
      headerMap.computeIfAbsent("Set-Cookie", k -> new ArrayList<>()).add(header.getValue());
    }
    if (!headerMap.isEmpty()) {
      try {
        cookieManager.put(uri, headerMap);
      } catch (IOException e) {
        LOG.warn("Failed to store cookies for URI {}", uri, e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    executor.shutdownNow();
    httpClient.close();
  }
}
