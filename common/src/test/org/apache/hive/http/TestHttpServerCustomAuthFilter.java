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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * End-to-end test that wires a sample {@link Filter} into HttpServer via the
 * {@code addGlobalFilter(name, pathSpec, Filter, initParams)} Builder API and
 * verifies that real HTTP requests to the running server flow through the
 * filter — and that the filter's decision (pass through or short-circuit with
 * 401) is honored.
 *
 * <p>This mirrors the wiring HiveServer2 performs at startup when
 * {@code hive.server2.webui.auth.method=CUSTOM} is set: instantiate the
 * configured filter class, read prefix-scoped init parameters off HiveConf,
 * then register the pair on the builder.
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
   * A passthrough custom filter registered against the global filter map
   * sees every request to the root webapp AND receives its configured init
   * parameters.
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
        .addGlobalFilter("custom-auth-filter", "/*", new RecordingFilter(), params)
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
        .addGlobalFilter("custom-auth-filter", "/*", new BlockingFilter())
        .build();
    server.start();

    int code = doGet("/jmx");
    assertEquals("Blocking filter should short-circuit with 401", 401, code);
    assertTrue("Filter should have run before blocking",
        BlockingFilter.callCount.get() >= 1);
  }

  /**
   * Without any global filter registered, requests proceed normally; the
   * recording filter sees nothing even though it is present on the classpath.
   */
  @Test(timeout = 30_000)
  public void testCustomAuthFilterNotInstalledWhenAbsent() throws Exception {
    int port = freePort();
    server = new HttpServer.Builder("test")
        .setConf(new HiveConf())
        .setHost("localhost")
        .setPort(port)
        .build();
    server.start();

    int code = doGet("/jmx");
    assertEquals(200, code);
    assertEquals("Filter must not be installed when not registered on the builder",
        0, RecordingFilter.callCount.get());
  }

  /**
   * Reproduces HiveServer2's config-driven wiring path: read the filter class
   * name and prefix-scoped init params straight off {@link HiveConf} via
   * {@code getPropsWithPrefix}, then hand both to the className-based
   * {@code addGlobalFilter} overload. The Builder owns instantiation, so the
   * caller code stays free of reflection.
   */
  @Test(timeout = 30_000)
  public void testCustomAuthFilterWiredFromHiveConfPropsWithPrefix() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_AUTH_METHOD.varname, "CUSTOM");
    conf.set(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_CUSTOM_AUTH_FILTER.varname,
        RecordingFilter.class.getName());
    String paramPrefix =
        HiveConf.ConfVars.HIVE_SERVER2_WEBUI_CUSTOM_AUTH_FILTER.varname + ".param.";
    conf.set(paramPrefix + "realm", "hive");
    conf.set(paramPrefix + "ttl", "600");

    String authFilter = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_CUSTOM_AUTH_FILTER);
    Map<String, String> params = conf.getPropsWithPrefix(paramPrefix);

    int port = freePort();
    server = new HttpServer.Builder("test")
        .setConf(conf)
        .setHost("localhost")
        .setPort(port)
        .addGlobalFilter("custom-auth-filter", "/*", authFilter, params)
        .build();
    server.start();

    int code = doGet("/jmx");
    assertEquals(200, code);
    assertTrue("Filter should have been invoked",
        RecordingFilter.callCount.get() >= 1);
    assertEquals("realm param flowed through from HiveConf to filter init",
        "hive", RecordingFilter.initParams.get("realm"));
    assertEquals("ttl param flowed through from HiveConf to filter init",
        "600", RecordingFilter.initParams.get("ttl"));
  }

  /**
   * The {@code /logs} endpoint is mounted on a sibling {@code ServletContextHandler},
   * not on the root webapp context, so it does not automatically inherit filters
   * registered against the webapp. This test asserts that a global custom auth
   * filter still applies to {@code /logs} — leaving log files unauthenticated
   * would defeat the point of enabling WebUI auth at all.
   */
  @Test(timeout = 30_000)
  public void testCustomAuthFilterAppliedToLogsEndpoint() throws Exception {
    Path logDir = Files.createTempDirectory("hs2-webui-logs-");

    HiveConf conf = new HiveConf();
    conf.set("hive.log.dir", logDir.toAbsolutePath().toString());

    int port = freePort();
    server = new HttpServer.Builder("test")
        .setConf(conf)
        .setHost("localhost")
        .setPort(port)
        .addGlobalFilter("custom-auth-filter", "/*", new RecordingFilter())
        .build();
    server.start();

    doGet("/logs/");

    assertTrue("Custom auth filter must intercept /logs as well; was "
        + RecordingFilter.callCount.get(), RecordingFilter.callCount.get() >= 1);
  }

  /**
   * The blocking variant: a deny-by-default filter on {@code /logs} must
   * short-circuit with 401 before the log-serving servlet runs.
   */
  @Test(timeout = 30_000)
  public void testCustomAuthFilterCanBlockLogsEndpoint() throws Exception {
    Path logDir = Files.createTempDirectory("hs2-webui-logs-");

    HiveConf conf = new HiveConf();
    conf.set("hive.log.dir", logDir.toAbsolutePath().toString());

    int port = freePort();
    server = new HttpServer.Builder("test")
        .setConf(conf)
        .setHost("localhost")
        .setPort(port)
        .addGlobalFilter("custom-auth-filter", "/*", new BlockingFilter())
        .build();
    server.start();

    int code = doGet("/logs/");
    assertEquals("Blocking filter must short-circuit /logs with 401", 401, code);
    assertTrue("Filter should have run before blocking",
        BlockingFilter.callCount.get() >= 1);
  }

  /**
   * The className overload defers class resolution to Jetty's lifecycle:
   * Builder.addGlobalFilter just stores the name, FilterHolder.doStart()
   * loads the class. So a bad class name must surface as a startup failure
   * (server.start()) — not silently — and the resulting stack should name
   * the offending class so the operator can fix the config.
   */
  @Test(timeout = 30_000)
  public void testCustomAuthFilterRejectsBadClassName() throws Exception {
    int port = freePort();
    server = new HttpServer.Builder("test")
        .setConf(new HiveConf())
        .setHost("localhost")
        .setPort(port)
        .addGlobalFilter("custom-auth-filter", "/*",
            "com.example.NonExistentFilterClass", null)
        .build();

    try {
      server.start();
      fail("Expected server.start() to fail when the filter class is missing");
    } catch (Exception expected) {
      String trace = stackTraceAsString(expected);
      assertTrue("Failure should name the offending class somewhere in the trace; was:\n" + trace,
          trace.contains("com.example.NonExistentFilterClass"));
    } finally {
      // Server may be partially started — make sure we don't leak the connector.
      try { server.stop(); } catch (Exception ignore) { }
      server = null;
    }
  }

  private static String stackTraceAsString(Throwable t) {
    java.io.StringWriter sw = new java.io.StringWriter();
    t.printStackTrace(new java.io.PrintWriter(sw));
    return sw.toString();
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
