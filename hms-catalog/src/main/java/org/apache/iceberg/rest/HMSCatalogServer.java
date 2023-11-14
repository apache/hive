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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.PropertyServlet;
import org.apache.hadoop.hive.metastore.SecureServletCaller;
import org.apache.hadoop.hive.metastore.ServletSecurity;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.util.PropertyUtil;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlet.Source;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class HMSCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogServer.class);
  private static final String CATALOG_ENV_PREFIX = "CATALOG_";

  private HMSCatalogServer() {
    // nothing
  }

  private static Catalog backendCatalog() throws IOException {
    // Translate environment variable to catalog properties
    Map<String, String> catalogProperties =
        System.getenv().entrySet().stream()
            .filter(e -> e.getKey().startsWith(CATALOG_ENV_PREFIX))
            .collect(
                Collectors.toMap(
                    e ->
                        e.getKey()
                            .replaceFirst(CATALOG_ENV_PREFIX, "")
                            .replaceAll("__", "-")
                            .replaceAll("_", ".")
                            .toLowerCase(Locale.ROOT),
                    Map.Entry::getValue,
                    (m1, m2) -> {
                      throw new IllegalArgumentException("Duplicate key: " + m1);
                    },
                    HashMap::new));

    // Fallback to a JDBCCatalog impl if one is not set
    catalogProperties.putIfAbsent(
        CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.jdbc.JdbcCatalog");
    catalogProperties.putIfAbsent(
        CatalogProperties.URI, "jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory");

    // Configure a default location if one is not specified
    String warehouseLocation = catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION);

    if (warehouseLocation == null) {
      File tmp = java.nio.file.Files.createTempDirectory("iceberg_warehouse").toFile();
      tmp.deleteOnExit();
      warehouseLocation = tmp.toPath().resolve("iceberg_data").toFile().getAbsolutePath();
      catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);

      LOG.info("No warehouse location set.  Defaulting to temp location: {}", warehouseLocation);
    }

    LOG.info("Creating catalog with properties: {}", catalogProperties);
    return CatalogUtil.buildIcebergCatalog("rest_backend", catalogProperties, new Configuration());
  }

  public static HttpServlet createServlet(SecureServletCaller security, Catalog catalog) throws IOException {
    try (HMSCatalogAdapter adapter =
        new HMSCatalogAdapter(catalog == null? backendCatalog() : catalog)) {
      return new HMSCatalogServlet(security, adapter);
    }
  }

  public static HttpServlet createServlet(Configuration configuration, HMSCatalog catalog) throws MetaException, IOException {
    String auth = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH);
    boolean jwt = auth != null && "jwt".equals(auth.toLowerCase());
    SecureServletCaller security = new ServletSecurity(configuration, jwt);
    return createServlet(security, catalog == null? new HMSCatalog(configuration) : catalog);
  }

  /**
   * Convenience method to start a http server that only serves this servlet.
   * @param conf the configuration
   * @return the server instance
   * @throws Exception if servlet initialization fails
   */

  public static Server startCatalogServer(Configuration conf, HMSCatalog catalog) throws Exception {
    HttpServlet servlet = createServlet(conf, catalog);
    int port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PORT);
    if (port < 0) {
      return null;
    }
    String cli = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PATH);
    //Server httpServer = PropertyServlet.startServer(conf, port, cli, servlet);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    ServletHolder servletHolder = new ServletHolder(servlet);
    servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
    context.addServlet(servletHolder, "/"+cli+"/*");
    context.setVirtualHosts(null);
    context.setGzipHandler(new GzipHandler());

    Server httpServer = new Server(port);
    httpServer.setHandler(context);

    httpServer.start();
    return httpServer;
  }

  public static void main(String[] args) throws Exception {
    Server httpServer = startCatalogServer(null, null);
    // start
    httpServer.join();
  }
}
