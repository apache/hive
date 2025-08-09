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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Helper class to ease the creation of embedded Jetty serving servlets on
 * different ports.
 */
public class ServletServerBuilder {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServletServerBuilder.class);
  private static final String HTTP = "http";
  private static final String HTTPS = "https";
  /**
   * The configuration instance.
   */
  private final Configuration configuration;
  /**
   * Keeping track of descriptors.
   */
  private final Map<Servlet, Descriptor> descriptorsMap = new IdentityHashMap<>();

  /**
   * Creates a builder instance.
   *
   * @param conf the configuration
   */
  public ServletServerBuilder(Configuration conf) {
    this.configuration = conf;
  }

  /**
   * Creates a builder.
   *
   * @param conf     the configuration
   * @param describe the functions to call that create servlet descriptors
   * @return the builder or null if no descriptors
   */
  @SafeVarargs
  public static ServletServerBuilder builder(Configuration conf,
                                             Function<Configuration, ServletServerBuilder.Descriptor>... describe) {
    List<ServletServerBuilder.Descriptor> descriptors = new ArrayList<>();
    Arrays.asList(describe).forEach(functor -> {
      ServletServerBuilder.Descriptor descriptor = functor.apply(conf);
      if (descriptor != null) {
        descriptors.add(descriptor);
      }
    });
    if (!descriptors.isEmpty()) {
      ServletServerBuilder builder = new ServletServerBuilder(conf);
      descriptors.forEach(builder::addServlet);
      return builder;
    }
    return null;
  }

  /**
   * Helper for the generic use case.
   *
   * @param logger   the logger
   * @param conf     the configuration
   * @param describe the functions to create descriptors
   * @return a server instance
   */
  @SafeVarargs
  public static Server startServer(
          Logger logger,
          Configuration conf,
          Function<Configuration, ServletServerBuilder.Descriptor>... describe) {
    return Objects.requireNonNull(builder(conf, describe)).start(logger);
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Adds a servlet instance.
   * <p>The servlet port can be shared between servlets; if 0, the system will provide
   * a port. If the port is &lt; 0, the system will provide a port dedicated (i.e., non-shared)
   * to the servlet.</p>
   *
   * @param port    the servlet port
   * @param path    the servlet path
   * @param servlet a servlet instance
   * @return a descriptor
   */
  public Descriptor addServlet(int port, String path, HttpServlet servlet) {
    Descriptor descriptor = new Descriptor(port, path, servlet);
    return addServlet(descriptor);
  }

  /**
   * Adds a servlet instance.
   *
   * @param descriptor a descriptor
   * @return the descriptor
   */
  public Descriptor addServlet(Descriptor descriptor) {
    if (descriptor != null) {
      descriptorsMap.put(descriptor.getServlet(), descriptor);
    }
    return descriptor;
  }

  /**
   * Creates a server instance.
   * <p>Default use configuration to determine thread-pool constants?</p>
   *
   * @return the server instance
   */
  private Server createServer() {
    final int maxThreads = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.HTTPSERVER_THREADPOOL_MAX);
    final int minThreads = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.HTTPSERVER_THREADPOOL_MIN);
    final int idleTimeout = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.HTTPSERVER_THREADPOOL_IDLE);
    final QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);
    Server server = new Server(threadPool);
    server.setStopAtShutdown(true);
    return server;
  }

  /**
   * Create an SSL context factory using the configuration.
   *
   * @param conf The configuration to use
   * @return The created SslContextFactory or null if creation failed
   */
  private static SslContextFactory createSslContextFactory(Configuration conf) throws IOException {
      return ServletSecurity.createSslContextFactoryIf(conf, MetastoreConf.ConfVars.HTTPSERVER_USE_HTTPS);
  }

  /**
   * Create an HTTP or HTTPS connector.
   *
   * @param server The server to create the connector for
   * @param sslContextFactory The ssl context factory to use;
   *                          if null, the connector will be HTTP; if not null, the connector will be HTTPS
   * @param port The port to bind the connector to
   * @return The created ServerConnector
   */
  private ServerConnector createConnector(Server server, SslContextFactory sslContextFactory, int port) {
    final ServerConnector connector;
    if (sslContextFactory == null) {
      connector = new ServerConnector(server);
      connector.setName(HTTP);
      HttpConnectionFactory httpFactory = connector.getConnectionFactory(HttpConnectionFactory.class);
      // do not leak information
      if (httpFactory != null) {
        HttpConfiguration httpConf = httpFactory.getHttpConfiguration();
        httpConf.setSendServerVersion(false);
        httpConf.setSendXPoweredBy(false);
      }
    } else {
      HttpConfiguration httpsConf = new HttpConfiguration();
      httpsConf.setSecureScheme(HTTPS);
      httpsConf.setSecurePort(port);
      // do not leak information
      httpsConf.setSendServerVersion(false);
      httpsConf.setSendXPoweredBy(false);
      httpsConf.addCustomizer(new SecureRequestCustomizer());
      connector = new ServerConnector(server, sslContextFactory, new HttpConnectionFactory(httpsConf));
      connector.setName(HTTPS);
    }
    connector.setPort(port);
    connector.setReuseAddress(true);
    return connector;
  }

  /**
   * Adds a servlet to its intended servlet context context.
   *
   * @param handlersMap the map of port to handlers
   * @param descriptor  the servlet descriptor
   */
  private void addServlet(Map<Integer, ServletContextHandler> handlersMap, Descriptor descriptor) {
    final int port = descriptor.getPort();
    final String path = descriptor.getPath();
    final HttpServlet servlet = descriptor.getServlet();
    // if port is < 0, use one for this servlet only
    int key = port < 0 ? -1 - handlersMap.size() : port;
    ServletContextHandler handler = handlersMap.computeIfAbsent(key, p -> {
      ServletContextHandler servletHandler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
      servletHandler.setContextPath("/");
      servletHandler.setGzipHandler(new GzipHandler());
      return servletHandler;
    });
    ServletHolder servletHolder = new ServletHolder(servlet);
    servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
    handler.addServlet(servletHolder, "/" + path + "/*");
  }

  /**
   * Convenience method to start an http server that serves all configured
   * servlets.
   *
   * @return the server instance or null if no servlet was configured
   * @throws Exception if servlet initialization fails
   */
  public Server startServer() throws Exception {
    // add all servlets
    Map<Integer, ServletContextHandler> handlersMap = new HashMap<>();
    for (Descriptor descriptor : descriptorsMap.values()) {
      addServlet(handlersMap, descriptor);
    }
    final int size = handlersMap.size();
    if (size == 0) {
      return null;
    }
    final Server server = createServer();
    // create the connectors
    final SslContextFactory sslContextFactory = createSslContextFactory(configuration);
    final ServerConnector[] connectors = new ServerConnector[size];
    final ServletContextHandler[] handlers = new ServletContextHandler[size];
    Iterator<Map.Entry<Integer, ServletContextHandler>> it = handlersMap.entrySet().iterator();
    for (int c = 0; it.hasNext(); ++c) {
      Map.Entry<Integer, ServletContextHandler> entry = it.next();
      int key = entry.getKey();
      int port = Math.max(key, 0);
      ServerConnector connector = createConnector(server, sslContextFactory, port);
      LOGGER.info("Adding {} servlet connector on port {}", connector.getName(), port);
      connectors[c] = connector;
      ServletContextHandler handler = entry.getValue();
      handlers[c] = handler;
      // make each servlet context be served only by its dedicated connector
      String host = "hms" + c;
      connector.setName(host);
      handler.setVirtualHosts(new String[]{"@" + host});
    }
    // hook the connectors and the handlers
    server.setConnectors(connectors);
    HandlerCollection portHandler = new ContextHandlerCollection();
    portHandler.setHandlers(handlers);
    server.setHandler(portHandler);
    // start the server
    server.start();
    // collect automatically assigned connector ports
    for (int i = 0; i < connectors.length; ++i) {
      int port = connectors[i].getLocalPort();
      ServletContextHandler handler = handlers[i];
      ServletHolder[] holders = handler.getServletHandler().getServlets();
      for (ServletHolder holder : holders) {
        Servlet servlet = holder.getServletInstance();
        if (servlet != null) {
          Descriptor descriptor = descriptorsMap.get(servlet);
          if (descriptor != null) {
            descriptor.setPort(port);
          }
        }
      }
    }
    return server;
  }

  /**
   * Creates and starts the server.
   *
   * @param logger a logger to output info
   * @return the server instance (or null if an error occurred)
   */
  public Server start(Logger logger) {
    try {
      Server server = startServer();
      if (server != null) {
        if (!server.isStarted()) {
          logger.error("Unable to start servlet server on {}", server.getURI());
        } else {
          descriptorsMap.values().forEach(descriptor -> logger.info("Started {} servlet on {}:{}",
                  descriptor,
                  descriptor.getPort(),
                  descriptor.getPath()));
        }
      }
      return server;
    } catch (Exception e) {
      logger.error("Unable to start servlet server", e);
      return null;
    }
  }

  /**
   * A descriptor of a servlet.
   * <p>After the server is started, unspecified port will be updated to reflect
   * what the system allocated.</p>
   */
  public static class Descriptor {
    private final String path;
    private final HttpServlet servlet;
    private int port;

    /**
     * Create a servlet descriptor.
     *
     * @param port    the servlet port (or 0 if the port is to be chosen by the system)
     * @param path    the servlet path
     * @param servlet the servlet instance
     */
    public Descriptor(int port, String path, HttpServlet servlet) {
      this.port = port;
      this.path = path;
      this.servlet = servlet;
    }

    @Override
    public String toString() {
      String name = null;
      try {
        name = servlet.getServletName() + ":" + port + "/" + path;
      } catch (IllegalStateException ill) {
        // ignore, it may happen if servlet config is not set (yet)
      }
      if (name == null) {
        name = servlet.getClass().getSimpleName();
      }
      return name + ":" + port + "/" + path;
    }

    public int getPort() {
      return port;
    }

    void setPort(int port) {
      this.port = port;
    }

    public String getPath() {
      return path;
    }

    public HttpServlet getServlet() {
      return servlet;
    }
  }
}

