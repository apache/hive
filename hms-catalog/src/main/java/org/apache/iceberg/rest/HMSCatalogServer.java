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
import org.apache.hadoop.hive.metastore.SecureServletCaller;
import org.apache.hadoop.hive.metastore.ServletSecurity;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.iceberg.catalog.Catalog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.http.HttpServlet;
import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Collections;

public class HMSCatalogServer {
  private static Reference<Catalog> catalogRef;
  static Catalog getLastCatalog() {
    return catalogRef != null? catalogRef.get() :  null;
  }

  private HMSCatalogServer() {
    // nothing
  }

  public static HttpServlet createServlet(SecureServletCaller security, Catalog catalog) throws IOException {
    try (HMSCatalogAdapter adapter = new HMSCatalogAdapter(catalog)) {
      return new HMSCatalogServlet(security, adapter);
    }
  }

  public static HttpServlet createServlet(Configuration configuration, Catalog catalog) throws IOException {
    String auth = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH);
    boolean jwt = "jwt".equalsIgnoreCase(auth);
    SecureServletCaller security = new ServletSecurity(configuration, jwt);
    Catalog actualCatalog = catalog;
    if (actualCatalog == null) {
      actualCatalog = new HMSCatalog(configuration);
      actualCatalog.initialize("hive", Collections.emptyMap());
    }
    catalogRef = new WeakReference<>(actualCatalog);
    return createServlet(security, actualCatalog);
  }

  /**
   * Convenience method to start a http server that only serves this servlet.
   * @param conf the configuration
   * @return the server instance
   * @throws Exception if servlet initialization fails
   */

  public static Server startServer(Configuration conf, HMSCatalog catalog) throws Exception {
    int port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PORT);
    if (port < 0) {
      return null;
    }
    final HttpServlet servlet = createServlet(conf, catalog);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    ServletHolder servletHolder = new ServletHolder(servlet);
    servletHolder.setInitParameter("javax.ws.rs.Application", "ServiceListPublic");
    final String cli = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PATH);
    context.addServlet(servletHolder, "/"+cli+"/*");
    context.setVirtualHosts(null);
    context.setGzipHandler(new GzipHandler());

    final Server httpServer = new Server(port);
    httpServer.setHandler(context);
    httpServer.start();
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
    return startServer(conf, null);
  }
}
