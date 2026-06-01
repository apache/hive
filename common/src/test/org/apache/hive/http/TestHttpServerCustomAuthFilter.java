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
package org.apache.hive.http;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end test that wires a sample {@link Filter} into HttpServer via
 * the custom-auth-filter Builder API and verifies that real HTTP requests
 * to the running server flow through the filter — and that the filter's
 * decision (pass through or short-circuit with 401) is honored.
 */
public class TestHttpServerCustomAuthFilter {

  private HttpServer server;

  @Before
  public void resetCounters() {
    RecordingFilter.reset();
    BlockingFilter.reset();
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.stop();
      server = null;
    }
  }

  /**
   * With {@code useCustomAuthFilter=true} and a passthrough filter, requests
   * reach the underlying servlet AND the filter sees them, including the
   * configured init parameters.
   */
  @Test(timeout = 30_000)
  public void testCustomAuthFilterInterceptsRequests() throws Exception {
    Map<String, String> params = new HashMap<>();
    params.put("realm", "hive");
    params.put("ttl", "600");

    int port = freePort();
    server = new HttpServer.Builder("test")
        .setConf(new HiveConf())
        .setHost("localhost")
        .setPort(port)
        .setUseCustomAuthFilter(true)
        .setCustomAuthFilter(RecordingFilter.class.getName())
        .setCustomAuthFilterParams(params)
        .build();
    server.start();

    int code = doGet("/jmx");
    assertEquals("Passthrough filter should not block the request", 200, code);

    assertTrue("Filter should have been invoked at least once; was "
        + RecordingFilter.callCount.get(), RecordingFilter.callCount.get() >= 1);
    assertEquals("Init params should be threaded through to the filter",
        "hive", RecordingFilter.initParams.get("realm"));
    assertEquals("600", RecordingFilter.initParams.get("ttl"));
  }

  /**
   * The filter's decision is authoritative: when the filter short-circuits
   * with a 401, the underlying servlet is never reached and the client
   * receives the 401 response code.
   */
  @Test(timeout = 30_000)
  public void testCustomAuthFilterCanBlockRequest() throws Exception {
    int port = freePort();
    server = new HttpServer.Builder("test")
        .setConf(new HiveConf())
        .setHost("localhost")
        .setPort(port)
        .setUseCustomAuthFilter(true)
        .setCustomAuthFilter(BlockingFilter.class.getName())
        .setCustomAuthFilterParams(new HashMap<>())
        .build();
    server.start();

    int code = doGet("/jmx");
    assertEquals("Blocking filter should short-circuit with 401", 401, code);
    assertTrue("Filter should have run before blocking",
        BlockingFilter.callCount.get() >= 1);
  }

  /**
   * Without {@code useCustomAuthFilter=true}, no custom filter is installed
   * and requests proceed normally; the recording filter sees nothing even
   * though it is present on the classpath.
   */
  @Test(timeout = 30_000)
  public void testCustomAuthFilterNotInstalledWhenDisabled() throws Exception {
    int port = freePort();
    server = new HttpServer.Builder("test")
        .setConf(new HiveConf())
        .setHost("localhost")
        .setPort(port)
        .build();
    server.start();

    int code = doGet("/jmx");
    assertEquals(200, code);
    assertEquals("Filter must not be installed when useCustomAuthFilter is off",
        0, RecordingFilter.callCount.get());
  }

  // ---- helpers -------------------------------------------------------------

  /**
   * Picks a currently-free port. HttpServer's PortHandlerWrapper keys handlers
   * by the configured port, so we cannot pass 0 (dynamic) — the actual bound
   * port would not match the registered handler. There is a small race window
   * between closing this socket and Jetty binding, which is acceptable for a
   * unit test.
   */
  private static int freePort() throws IOException {
    try (ServerSocket s = new ServerSocket(0)) {
      return s.getLocalPort();
    }
  }

  private int doGet(String path) throws IOException {
    URL url = new URL("http://localhost:" + server.getPort() + path);
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    try {
      conn.setRequestMethod("GET");
      conn.setConnectTimeout(5_000);
      conn.setReadTimeout(5_000);
      return conn.getResponseCode();
    } finally {
      conn.disconnect();
    }
  }

  // ---- sample filters ------------------------------------------------------

  /** Records every invocation and the init parameters seen at startup. */
  public static class RecordingFilter implements Filter {
    static final AtomicInteger callCount = new AtomicInteger(0);
    static volatile Map<String, String> initParams = new HashMap<>();

    static void reset() {
      callCount.set(0);
      initParams = new HashMap<>();
    }

    @Override
    public void init(FilterConfig fc) {
      Map<String, String> seen = new HashMap<>();
      Enumeration<String> names = fc.getInitParameterNames();
      while (names.hasMoreElements()) {
        String n = names.nextElement();
        seen.put(n, fc.getInitParameter(n));
      }
      initParams = seen;
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
        throws IOException, ServletException {
      callCount.incrementAndGet();
      chain.doFilter(req, resp);
    }

    @Override
    public void destroy() {
    }
  }

  /** Short-circuits every request with 401, like a deny-by-default auth filter. */
  public static class BlockingFilter implements Filter {
    static final AtomicInteger callCount = new AtomicInteger(0);

    static void reset() {
      callCount.set(0);
    }

    @Override
    public void init(FilterConfig fc) {
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
        throws IOException, ServletException {
      callCount.incrementAndGet();
      ((HttpServletResponse) resp).sendError(HttpServletResponse.SC_UNAUTHORIZED, "blocked");
    }

    @Override
    public void destroy() {
    }
  }
}
