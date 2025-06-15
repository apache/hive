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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.hive.HiveCatalog;

/**
 * Catalog &amp; servlet factory.
 */
public class HMSCatalogFactory {
  /**
   * Convenience soft reference to last catalog.
   */
  private static final AtomicReference<Reference<HiveCatalog>> catalogRef = new AtomicReference<>();

  public static HiveCatalog getLastCatalog() {
    Reference<HiveCatalog> soft = catalogRef.get();
    return soft != null ? soft.get() : null;
  }

  private static void setLastCatalog(HiveCatalog catalog) {
    catalogRef.set(new SoftReference<>(catalog));
  }

  private final Configuration configuration;
  private final int port;
  private final String path;
  private HiveCatalog catalog;

  /**
   * Factory constructor.
   * <p>Called by the static method {@link HMSCatalogFactory#createServlet(Configuration)} that is
   * declared in configuration and found through introspection.</p>
   * @param conf the configuration
   */
  private HMSCatalogFactory(Configuration conf) {
    port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PORT);
    path = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
    this.configuration = conf;
  }
  
  public int getPort() {
    return port;
  }
  
  public String getPath() {
    return path;
  }

  /**
   * Creates the catalog instance.
   * @return the catalog
   */
  private HiveCatalog createCatalog() {
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
    final HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
    hiveCatalog.setConf(configuration);
    final String catalogName = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    hiveCatalog.initialize(catalogName, properties);
    return hiveCatalog;
  }

  /**
   * Creates the REST catalog servlet instance.
   * @param catalog the Iceberg catalog
   * @return the servlet
   */
  private HttpServlet createServlet(HiveCatalog catalog) {
    String authType = MetastoreConf.getVar(configuration, ConfVars.ICEBERG_CATALOG_SERVLET_AUTH);
    ServletSecurity security = new ServletSecurity(authType, configuration);
    return security.proxy(new HMSCatalogServlet(new HMSCatalogAdapter(catalog)));
  }

  /**
   * Creates the REST catalog servlet instance.
   * @return the servlet
   */
  private HttpServlet createServlet() {
    if (port >= 0 && path != null && !path.isEmpty()) {
      HiveCatalog actualCatalog = catalog;
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
   * <p>This method name is found through configuration as {@link MetastoreConf.ConfVars#ICEBERG_CATALOG_SERVLET_FACTORY}
   * and looked up through reflection to start from HMS.</p>
   *
   * @param configuration the configuration
   * @return the servlet descriptor instance
   */
  @SuppressWarnings("unused")
  public static ServletServerBuilder.Descriptor createServlet(Configuration configuration) {
    HMSCatalogFactory hms = new HMSCatalogFactory(configuration);
    HttpServlet servlet = hms.createServlet();
    if (servlet == null) {
      return null;
    }
    return new ServletServerBuilder.Descriptor(hms.getPort(), hms.getPath(), servlet);
  }
}
