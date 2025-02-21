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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg Catalog server creator.
 */
public class HMSCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogServer.class);
  protected static final AtomicReference<Reference<Catalog>> catalogRef = new AtomicReference<>();

  public static Catalog getLastCatalog() {
    Reference<Catalog> soft = catalogRef.get();
    return soft != null ? soft.get() : null;
  }

  protected static void setLastCatalog(Catalog catalog) {
    catalogRef.set(new SoftReference<>(catalog));
  }

  protected final Configuration configuration;
  protected final int port;
  protected final String path;
  protected Catalog catalog;

  protected HMSCatalogServer(Configuration conf, Catalog catalog) {
    port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PORT);
    path = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
    this.configuration = conf;
    this.catalog = catalog;
  }
  
  public int getPort() {
    return port;
  }
  
  public String getPath() {
    return path;
  }
  
  public Catalog getCatalog() {
    return catalog;
  }

  protected Catalog createCatalog() {
    final Map<String, String> properties = new TreeMap<>();
    MetastoreConf.setVar(configuration, MetastoreConf.ConfVars.THRIFT_URIS, "");
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

  protected HttpServlet createServlet() throws IOException {
    if (port >= 0 && path != null && !path.isEmpty()) {
      Catalog actualCatalog = catalog;
      if (actualCatalog == null) {
        actualCatalog = catalog = createCatalog();
      }
      setLastCatalog(actualCatalog);
      return createServlet(actualCatalog);
    }
    return null;
  }

  
  /**
   * Factory method to describe Iceberg servlet.
   * <p>This one is looked up through reflection to start from HMS.</p>
   *
   * @param conf the configuration
   * @return the servlet descriptor instance
   */
  public static ServletServerBuilder.Descriptor createServlet(Configuration configuration) {
    try {
      HMSCatalogServer hms = new HMSCatalogServer(configuration, null);
      HttpServlet servlet = hms.createServlet();
      if (servlet != null) {
        return new ServletServerBuilder.Descriptor(hms.getPort(), hms.getPath(), servlet) {
          @Override public String toString() {
            return "Iceberg REST Catalog";
          }
        };
      }
    } catch (Exception exception) {
      LOG.error("failed to create servlet ", exception);
    }
    return null;
  }
}
