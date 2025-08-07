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

import java.util.Map;
import java.util.TreeMap;
import javax.servlet.http.HttpServlet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ServletSecurity;
import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.ServletServerBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hive.HiveCatalog;

/**
 * Catalog &amp; servlet factory.
 * <p>This class is derivable on purpose; the factory class name is a configuration property, this class
 * can serve as a base for specialization.</p>
 */
public final class HMSCatalogFactory {
  private static final String SERVLET_ID_KEY = "metastore.in.test.iceberg.catalog.servlet.id";

  private final Configuration configuration;
  private final int port;
  private final String path;

  /**
   * Factory constructor.
   * <p>Called by the static method {@link HMSCatalogFactory#createServlet(Configuration)} that is
   * declared in configuration and found through introspection.</p>
   * @param conf the configuration
   */
  private HMSCatalogFactory(Configuration conf) {
    port = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.CATALOG_SERVLET_PORT);
    path = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
    this.configuration = conf;
  }

  private int getPort() {
    return port;
  }

  private String getPath() {
    return path;
  }

  /**
   * Creates the catalog instance.
   * @return the catalog
   */
  private Catalog createCatalog() {
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
    if (configuration.get(SERVLET_ID_KEY) != null) {
      // For the testing purpose. HiveCatalog caches a metastore client in a static field. As our tests can spin up
      // multiple HMS instances, we need a unique cache key.
      properties.put(CatalogProperties.CLIENT_POOL_CACHE_KEYS, String.format("conf:%s", SERVLET_ID_KEY));
    }
    final HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
    hiveCatalog.setConf(configuration);
    final String catalogName = MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.CATALOG_DEFAULT);
    hiveCatalog.initialize(catalogName, properties);
    long expiry = MetastoreConf.getLongVar(configuration, MetastoreConf.ConfVars.ICEBERG_CATALOG_CACHE_EXPIRY);
    return expiry > 0 ? new HMSCachingCatalog(hiveCatalog, expiry) : hiveCatalog;
  }

  /**
   * Creates the REST catalog servlet instance.
   * @param catalog the Iceberg catalog
   * @return the servlet
   */
  private HttpServlet createServlet(Catalog catalog) {
    String authType = MetastoreConf.getVar(configuration, ConfVars.CATALOG_SERVLET_AUTH);
    ServletSecurity security = new ServletSecurity(AuthType.fromString(authType), configuration);
    return security.proxy(new HMSCatalogServlet(new HMSCatalogAdapter(catalog)));
  }

  /**
   * Creates the REST catalog servlet instance.
   * @return the servlet
   */
  private HttpServlet createServlet() {
    if (port >= 0 && path != null && !path.isEmpty()) {
      Catalog actualCatalog = createCatalog();
      return createServlet(actualCatalog);
    }
    return null;
  }
  
  /**
   * Factory method to describe Iceberg servlet.
   * <p>This method name is found through configuration as {@link MetastoreConf.ConfVars#CATALOG_SERVLET_FACTORY}
   * and looked up through reflection to start from HMS.</p>
   *
   * @param configuration the configuration
   * @return the servlet descriptor instance
   */
  @SuppressWarnings("unused")
  public static ServletServerBuilder.Descriptor createServlet(Configuration configuration) {
    HMSCatalogFactory hms = new HMSCatalogFactory(configuration);
    HttpServlet servlet = hms.createServlet();
    if (servlet != null) {
      return new ServletServerBuilder.Descriptor(hms.getPort(), hms.getPath(), servlet);
    }
    return null;
  }
}
