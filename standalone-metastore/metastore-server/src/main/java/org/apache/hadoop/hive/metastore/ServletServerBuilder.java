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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
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

/**
 * Helper class to ease creation of embedded Jetty serving servlets on
 * different ports.
 */
public class ServletServerBuilder {
  /**
   * Keeping track of descriptors.
   */
  private Map<Servlet, Descriptor> descriptorsMap = new IdentityHashMap<>();
  /**
   * The configuration instance.
   */
  protected final Configuration configuration;

  /**
   * Creates a builder instance.
   *
   * @param conf the configuration
   */
  protected ServletServerBuilder(Configuration conf) {
    this.configuration = conf;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * A descriptor of a servlet.
   * <p>After server is started, unspecified port will be updated to reflect
   * what the system allocated.</p>
   */
  public static class Descriptor {
    private int port;
    private final String path;
    private final HttpServlet servlet;

    /**
     * Create a servlet descriptor.
     * @param port the servlet port (or 0 if system allocated)
     * @param path the servlet path
     * @param servlet the servlet instance
     */
    public Descriptor(int port, String path, HttpServlet servlet) {
      this.port = port;
      this.path = path;
      this.servlet = servlet;
    }
    
    public String toString() {
      return servlet.getClass().getSimpleName() + ":" + port+ "/"+ path ;
    }

    public int getPort() {
      return port;
    }

    public String getPath() {
      return path;
    }

    public HttpServlet getServlet() {
      return servlet;
    }
  }

  /**
   * Adds a servlet instance.
   * <p>The servlet port can be shared between servlets; if 0, the system will provide
   * a port. If the port is &lt; 0, the system will provide a port dedicated (ie non-shared)
   * to the servlet.</p>
   * @param port the servlet port
   * @param path the servlet path
   * @param servlet a servlet instance
   * @return a descriptor
   */
  public Descriptor addServlet(int port, String path, HttpServlet servlet){
    Descriptor descriptor  = new Descriptor(port, path, servlet);
    return addServlet(descriptor);
  }

  /**
   * Adds a servlet instance.
   *
   * @param descriptor a descriptor
   * @return the descriptor
   */
  public Descriptor addServlet(Descriptor descriptor){
    descriptorsMap.put(descriptor.getServlet(), descriptor);
    return descriptor;
  }

  /**
   * Creates a server instance.
   * <p>Default use configuration to determine thread-pool constants?</p>
   *
   * @return the server instance
   * @throws IOException if server creation fails
   */
  protected Server createServer() throws IOException {
    final int maxThreads = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.EMBEDDED_JETTY_THREADPOOL_MAX);
    final int minThreads = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.EMBEDDED_JETTY_THREADPOOL_MIN);
    final int idleTimeout = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.EMBEDDED_JETTY_THREADPOOL_IDLE);
    final QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);
    Server server = new Server(threadPool);
    server.setStopAtShutdown(true);
    return server;
  }

  /**
   * Creates a server instance and a connector on a given port.
   *
   * @param server the server instance
   * @param sslContextFactory the ssl factory
   * @param port the port
   * @return the server connector listening to the port
   * @throws IOException if server creation fails
   */
  protected ServerConnector createConnector(Server server, SslContextFactory sslContextFactory, int port) throws IOException {
    final ServerConnector connector = new ServerConnector(server, sslContextFactory);
    connector.setPort(port);
    connector.setReuseAddress(true);
    HttpConnectionFactory httpFactory = connector.getConnectionFactory(HttpConnectionFactory.class);
    // do not leak information
    if (httpFactory != null) {
      HttpConfiguration httpConf = httpFactory.getHttpConfiguration();
      httpConf.setSendServerVersion(false);
      httpConf.setSendXPoweredBy(false);
    }
    return connector;
  }

  /**
   * Adds a servlet to its intended servlet context handler.
   * @param handlersMap the map of port to handlers
   * @param descriptor the servlet descriptor
   * @throws IOException
   */
  protected void addServlet(Map<Integer, ServletContextHandler> handlersMap, Descriptor descriptor) throws IOException {
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
   * Convenience method to start a http server that serves all configured
   * servlets.
   *
   * @return the server instance or null if no servlet was configured
   * @throws Exception if servlet initialization fails
   */
  public Server startServer() throws Exception {
    // add all servlets
    Map<Integer, ServletContextHandler> handlersMap = new HashMap<>();
    for(Descriptor descriptor : descriptorsMap.values()) {
      addServlet(handlersMap, descriptor);
    }
    final int size = handlersMap.size();
    if (size == 0) {
      return null;
    }
    final Server server = createServer();
    // create the connectors
    final SslContextFactory sslFactory = ServletSecurity.createSslContextFactory(configuration);
    final int[] keys = new int[size];
    final ServerConnector[] connectors = new ServerConnector[size];
    final ServletContextHandler[] handlers = new ServletContextHandler[size];
    Iterator<Map.Entry<Integer, ServletContextHandler>> it = handlersMap.entrySet().iterator();
    for (int c = 0; it.hasNext(); ++c) {
      Map.Entry<Integer, ServletContextHandler> entry = it.next();
      int key = entry.getKey();
      keys[c] = key;
      int port = key < 0? 0 : key;
      ServerConnector connector = createConnector(server, sslFactory, port);
      connectors[c] = connector;
      ServletContextHandler handler = entry.getValue();
      handlers[c] = handler;
      // make each servlet context be served only by its dedicated connector
      String host = "hms" + Integer.toString(c);
      connector.setName(host);
      handler.setVirtualHosts(new String[]{"@"+host});
    }
    // hook the connectors and the handlers
    server.setConnectors(connectors);
    HandlerCollection portHandler = new ContextHandlerCollection();
    portHandler.setHandlers(handlers);
    server.setHandler(portHandler);
    // start the server
    server.start();
    // collect auto ports
    for (int i = 0; i < connectors.length; ++i) {
      int port = connectors[i].getLocalPort();
      ServletContextHandler handler = handlers[i];
      ServletHolder[] holders = handler.getServletHandler().getServlets();
      for(ServletHolder holder : holders) {
        Servlet servlet = holder.getServletInstance();
        if (servlet != null) {
          Descriptor descriptor = descriptorsMap.get(servlet);
          if (descriptor != null) {
            descriptor.port = port;
          }
        }
      }
    }
    return server;
  }

  /**
   * Helper for generic use case.
   * @param logger the logger
   * @param conf the configuration
   * @param describe the functions to create descriptors
   * @return a server instance
   */
  @SafeVarargs
  public static Server startServer(
          Logger logger,
          Configuration conf,
          Function<Configuration, ServletServerBuilder.Descriptor>... describe) {
    List<ServletServerBuilder.Descriptor> descriptors = new ArrayList();
    Arrays.asList(describe).forEach(functor -> {
      ServletServerBuilder.Descriptor descriptor = functor.apply(conf);
      if (descriptor != null) {
        descriptors.add(descriptor);
      };
    });
    if (!descriptors.isEmpty()) {
      ServletServerBuilder builder = new ServletServerBuilder(conf);
      descriptors.forEach(d -> builder.addServlet(d));
      try {
        Server server = builder.startServer();
        if (server != null) {
          if (!server.isStarted()) {
            logger.error("Unable to start property-maps servlet server on {}", server.getURI());
          } else {
            descriptors.forEach(descriptor -> {
            logger.info("Started {} servlet on {}:{}",
                    descriptor.toString(),
                   descriptor.getPort(),
                   descriptor.getPath());
            });
          }
        }
        return server;
      } catch(Exception exception) {
        logger.error("Unable to start servlet server", exception);
        return null;
      } catch(Throwable throwable) {
        logger.error("Unable to start servlet server", throwable);
        return null;
      }
    }
    return null;
  }
}

