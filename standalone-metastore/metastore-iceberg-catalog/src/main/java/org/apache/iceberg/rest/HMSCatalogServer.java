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
import org.apache.hadoop.hive.metastore.ServletServerBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg Catalog server creator.
 */
public class HMSCatalogServer extends ServletServerBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogServer.class);
  protected static final AtomicReference<Reference<Catalog>> catalogRef = new AtomicReference<>();

  public static Catalog getLastCatalog() {
    Reference<Catalog> soft = catalogRef.get();
    return soft != null ? soft.get() : null;
  }

  protected static void setLastCatalog(Catalog catalog) {
    catalogRef.set(new SoftReference<>(catalog));
  }

  protected final int port;
  protected final String path;
  protected Catalog catalog;

  protected HMSCatalogServer(Configuration conf, Catalog catalog) {
    super(conf);
    port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PORT);
    path = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
    this.catalog = catalog;
  }

  @Override
  protected String getServletPath() {
    return path;
  }

  @Override
  protected int getServerPort() {
    return port;
  }

  protected Catalog createCatalog() {
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
    return expiry > 0 ? new HMSCachingCatalog(catalog, expiry) : catalog;
  }

  protected HttpServlet createServlet(Catalog catalog) throws IOException {
    ServletSecurity security = new ServletSecurity(configuration);
    return security.proxy(new HMSCatalogServlet(new HMSCatalogAdapter(catalog)));
  }

  @Override
  protected HttpServlet createServlet() throws IOException {
    Catalog actualCatalog = catalog;
    if (actualCatalog == null) {
      MetastoreConf.setVar(configuration, MetastoreConf.ConfVars.THRIFT_URIS, "");
      actualCatalog = catalog = createCatalog();
    }
    setLastCatalog(actualCatalog);
    return createServlet(actualCatalog);
  }

  @Override
  protected Server createServer() {
    final int maxThreads = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.ICEBERG_CATALOG_JETTY_THREADPOOL_MAX);
    final int minThreads = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.ICEBERG_CATALOG_JETTY_THREADPOOL_MIN);
    final int idleTimeout = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.ICEBERG_CATALOG_JETTY_THREADPOOL_IDLE);
    final QueuedThreadPool threadPool = new QueuedThreadPool(maxThreads, minThreads, idleTimeout);
    return new Server(threadPool);
  }

  /**
   * Convenience method to start a http server that only serves the Iceberg
   * catalog servlet.
   * <p>
   * This one is looked up through reflection to start from HMS.</p>
   *
   * @param conf the configuration
   * @return the server instance
   * @throws Exception if servlet initialization fails
   */
  public static Server startServer(Configuration conf) throws Exception {
    Server server = new HMSCatalogServer(conf, null).startServer();
    if (server != null) {
      if (!server.isStarted()) {
        LOG.error("Unable to start property-maps servlet server on {}", server.getURI());
      } else {
        LOG.info("Started property-maps servlet server on {}", server.getURI());
      }
    }
    return server;
  }
}
