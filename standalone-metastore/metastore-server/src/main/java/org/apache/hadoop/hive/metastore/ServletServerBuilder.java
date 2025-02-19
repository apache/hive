/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;

/**
 * Helper class to ease creation of embedded Jetty serving one servlet on a given port.
 * <p>When using Jetty, the easiest way - and may be only - to serve different servlets
 * on different ports is to create 2 separate Jetty instances; this helper eases creation
 * of such a dedicated server.</p>
 */
public abstract class ServletServerBuilder {
  /**
   * The configuration instance.
   */
  protected final Configuration configuration;

  /**
   * Creates a builder instance.
   * @param conf the configuration
   */
  protected ServletServerBuilder(Configuration conf) {
    this.configuration = conf;
  }
  /**
   * Gets the servlet path.
   * @return the path
   */
  protected abstract String getServletPath();

  /**
   * Gets the server port.
   * @return the port
   */
  protected abstract int getServerPort();

  /**
   * Creates the servlet instance.
   * <p>It is often advisable to use {@link ServletSecurity} to proxy the actual servlet instance.</p>
   * @return the servlet instance
   * @throws IOException if servlet creation fails
   */
  protected abstract HttpServlet createServlet() throws IOException;

  /**
   * Creates the servlet context.
   * @param servlet the servlet
   * @return a context instance
   */
  protected ServletContextHandler createContext(HttpServlet servlet) {
    // hook the servlet
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    ServletHolder servletHolder = new ServletHolder(servlet);
    servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
    final String path = getServletPath();
    context.addServlet(servletHolder, "/" + path + "/*");
    context.setVirtualHosts(null);
    context.setGzipHandler(new GzipHandler());
    return context;
  }

  /**
   * Creates a server instance.
   * <p>Default use configuration to determine threadpool constants?</p>
   * @return the server instance
   * @throws IOException if server creation fails
   */
  protected Server createServer() throws IOException {
    final int maxThreads = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.EMBEDDED_JETTY_THREADPOOL_MAX);
    final int minThreads = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.EMBEDDED_JETTY_THREADPOOL_MIN);
    final int idleTimeout = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.EMBEDDED_JETTY_THREADPOOL_IDLE);
    final QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);
    return new Server(threadPool);
  }

  /**
   * Creates a server instance and a connector on a given port.
   * @param port the port
   * @return the server instance listening to the port
   * @throws IOException if server creation fails
   */
  protected Server createServer(int port) throws IOException {
    final Server server = createServer();
    server.setStopAtShutdown(true);
    final SslContextFactory sslContextFactory = ServletSecurity.createSslContextFactory(configuration);
    final ServerConnector connector = new ServerConnector(server, sslContextFactory);
    connector.setPort(port);
    connector.setReuseAddress(true);
    server.addConnector(connector);
    HttpConnectionFactory httpFactory = connector.getConnectionFactory(HttpConnectionFactory.class);
    // do not leak information
    if (httpFactory != null) {
      HttpConfiguration httpConf = httpFactory.getHttpConfiguration();
      httpConf.setSendServerVersion(false);
      httpConf.setSendXPoweredBy(false);
    }
    return server;
  }

  /**
   * Convenience method to start a http server that only serves this servlet.
   * @return the server instance or null if port &lt; 0
   * @throws Exception if servlet initialization fails
   */
  public Server startServer() throws Exception {
    int port = getServerPort();
    if (port < 0) {
      return null;
    }
    // create the servlet
    final HttpServlet servlet = createServlet();
    // hook the servlet
    ServletContextHandler context = createContext(servlet);
    // Http server
    final Server httpServer = createServer(port);
    httpServer.setHandler(context);
    httpServer.start();
    return httpServer;
  }

}
