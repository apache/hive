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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.conf.Configuration;
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
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogServer.class);
  private static final AtomicReference<Reference<Catalog>> catalogRef = new AtomicReference<>();

  public static Catalog getLastCatalog() {
    Reference<Catalog> soft = catalogRef.get();
    return soft != null ? soft.get() :  null;
  }
  
  protected static void setLastCatalog(Catalog catalog) {
    catalogRef.set(new SoftReference<>(catalog));
  }

  private HMSCatalogServer() {
    // nothing
  }

  protected HttpServlet createServlet(ServletSecurity security, Catalog catalog) throws IOException {
    return security.proxy(new HMSCatalogServlet(new HMSCatalogAdapter(catalog)));
  }

  protected Catalog createCatalog(Configuration configuration) {
    final Map<String, String> properties = new TreeMap<>();
    final String configUri = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.THRIFT_URIS);
    if (configUri != null) {
      properties.put("uri", configUri);
    }
    final String configWarehouse = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.WAREHOUSE);
    if (configWarehouse != null) {
      properties.put("warehouse", configWarehouse);
    }
    final String configExtWarehouse = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.WAREHOUSE_EXTERNAL);
    if (configExtWarehouse != null) {
      properties.put("external-warehouse", configExtWarehouse);
    }
    final HiveCatalog catalog = new org.apache.iceberg.hive.HiveCatalog();
    catalog.setConf(configuration);
    final String catalogName = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    catalog.initialize(catalogName, properties);
    long expiry = MetastoreConf.getLongVar(configuration, MetastoreConf.ConfVars.ICEBERG_CATALOG_CACHE_EXPIRY);
    return expiry > 0? new HMSCachingCatalog(catalog, expiry) : catalog;
  }

  protected HttpServlet createServlet(Configuration configuration, Catalog catalog) throws IOException {
    ServletSecurity security = new ServletSecurity(configuration);
    Catalog actualCatalog = catalog;
    if (actualCatalog == null) {
      MetastoreConf.setVar(configuration, MetastoreConf.ConfVars.THRIFT_URIS, "");
      actualCatalog = createCatalog(configuration);
    }
    setLastCatalog(actualCatalog);
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
    final int maxThreads = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_JETTY_THREADPOOL_MAX);
    final int minThreads = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_JETTY_THREADPOOL_MIN);
    final int idleTimeout = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_JETTY_THREADPOOL_IDLE);
    final QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);
    final Server httpServer = new Server(threadPool);
    httpServer.setStopAtShutdown(true);
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
