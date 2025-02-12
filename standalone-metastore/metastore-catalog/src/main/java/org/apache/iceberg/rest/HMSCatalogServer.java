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

package org.apache.iceberg.rest;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.SecureServletCaller;
import org.apache.hadoop.hive.metastore.ServletSecurity;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Iceberg Catalog server.
 */
public class HMSCatalogServer {
  private static final String CACHE_EXPIRY = "hive.metastore.catalog.cache.expiry";
  private static final String JETTY_THREADPOOL_MIN = "hive.metastore.catalog.jetty.threadpool.min";
  private static final String JETTY_THREADPOOL_MAX = "hive.metastore.catalog.jetty.threadpool.max";
  private static final String JETTY_THREADPOOL_IDLE = "hive.metastore.catalog.jetty.threadpool.idle";
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogServer.class);
  private static Reference<Catalog> catalogRef;

  static Catalog getLastCatalog() {
    return catalogRef != null ? catalogRef.get() :  null;
  }

  protected HMSCatalogServer() {
    // nothing
  }

  protected HttpServlet createServlet(SecureServletCaller security, Catalog catalog) throws IOException {
    return new HMSCatalogServlet(security, new HMSCatalogAdapter(catalog));
  }

  protected Catalog createCatalog(Configuration configuration) {
    final String configUri = configuration.get(MetastoreConf.ConfVars.THRIFT_URIS.getVarname());
    final String configWarehouse = configuration.get(MetastoreConf.ConfVars.WAREHOUSE.getVarname());
    final String configExtWarehouse = configuration.get(MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL.getVarname());
    Map<String, String> properties = new TreeMap<>();
    if (configUri != null) {
      properties.put("uri", configUri);
    }
    if (configWarehouse != null) {
      properties.put("warehouse", configWarehouse);
    }
    if (configExtWarehouse != null) {
      properties.put("external-warehouse", configExtWarehouse);
    }
    final HiveCatalog catalog = new org.apache.iceberg.hive.HiveCatalog();
    catalog.setConf(configuration);
    catalog.initialize("hive", properties);
    long expiry = configuration.getLong(CACHE_EXPIRY, 60_000L);
    return expiry > 0? new HMSCachingCatalog(catalog, expiry) : catalog;
  }

  protected HttpServlet createServlet(Configuration configuration, Catalog catalog) throws IOException {
    String auth = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_AUTH);
    boolean jwt = "jwt".equalsIgnoreCase(auth);
    SecureServletCaller security = new ServletSecurity(configuration, jwt);
    Catalog actualCatalog = catalog;
    if (actualCatalog == null) {
      // FIXME: 20250212 - the no-Thrift requires this but then test fail with closed Peristence Manager
      // MetastoreConf.setVar(configuration, MetastoreConf.ConfVars.THRIFT_URIS, "");
      actualCatalog = createCatalog(configuration);
    }
    catalogRef = new SoftReference<>(actualCatalog);
    return createServlet(security, actualCatalog);
  }

  /**
   * Convenience method to start a http server that only serves this servlet.
   * @param conf the configuration
   * @param catalog the catalog instance to serve
   * @return the server instance
   * @throws Exception if servlet initialization fails
   */
  protected Server startServer(Configuration conf, HiveCatalog catalog) throws Exception {
    int port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PORT);
    if (port < 0) {
      return null;
    }
    final HttpServlet servlet = createServlet(conf, catalog);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    ServletHolder servletHolder = new ServletHolder(servlet);
    servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
    final String cli = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
    context.addServlet(servletHolder, "/" + cli + "/*");
    context.setVirtualHosts(null);
    context.setGzipHandler(new GzipHandler());

    final Server httpServer = createHttpServer(conf, port);
    httpServer.setHandler(context);
    LOG.info("Starting HMS REST Catalog Server with context path:/{}/ on port:{}", cli, port);
    httpServer.start();
    return httpServer;
  }

  private static Server createHttpServer(Configuration conf, int port) throws IOException {
    final int maxThreads = conf.getInt(JETTY_THREADPOOL_MAX, 256);
    final int minThreads = conf.getInt(JETTY_THREADPOOL_MIN, 8);
    final int idleTimeout = conf.getInt(JETTY_THREADPOOL_IDLE, 60_000);
    final QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);
    final Server httpServer = new Server(threadPool);
    final SslContextFactory sslContextFactory = ServletSecurity.createSslContextFactory(conf);
    final ServerConnector connector = new ServerConnector(httpServer, sslContextFactory);
    connector.setPort(port);
    connector.setReuseAddress(true);
    httpServer.setConnectors(new Connector[] {connector});
    for (ConnectionFactory factory : connector.getConnectionFactories()) {
      if (factory instanceof HttpConnectionFactory) {
        HttpConnectionFactory httpFactory = (HttpConnectionFactory) factory;
        HttpConfiguration httpConf = httpFactory.getHttpConfiguration();
        httpConf.setSendServerVersion(false);
        httpConf.setSendXPoweredBy(false);
      }
    }
    return httpServer;
  }


  /**
   * Convenience method to start a http server that only serves this servlet.
   * <p>This one is looked up through reflection to start from HMS.</p>
   * @param conf the configuration
   * @return the server instance
   * @throws Exception if servlet initialization fails
   */
  public static Server startServer(Configuration conf) throws Exception {
    return new HMSCatalogServer().startServer(conf, null);
  }
}
