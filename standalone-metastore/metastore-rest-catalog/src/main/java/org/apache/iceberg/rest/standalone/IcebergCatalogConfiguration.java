/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iceberg.rest.standalone;

import javax.servlet.http.HttpServlet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.HMSCatalogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring configuration for the Iceberg REST Catalog servlet.
 * Extracted to separate concerns from the main application bootstrap.
 */
@org.springframework.context.annotation.Configuration
public class IcebergCatalogConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogConfiguration.class);

  private final Configuration conf;

  public IcebergCatalogConfiguration(Configuration conf) {
    this.conf = conf;
  }

  @Bean
  public ServletRegistrationBean<HttpServlet> restCatalogServlet() {
    // Determine servlet path and port
    String servletPath = MetastoreConf.getVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
    if (servletPath == null || servletPath.isEmpty()) {
      servletPath = "iceberg";
      MetastoreConf.setVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, servletPath);
    }

    int port = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
    if (port == 0) {
      port = 8080;
      MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, port);
    }

    LOG.info("Creating REST Catalog servlet at /{}", servletPath);

    // Create servlet from Iceberg factory
    org.apache.hadoop.hive.metastore.ServletServerBuilder.Descriptor descriptor =
        HMSCatalogFactory.createServlet(conf);
    if (descriptor == null || descriptor.getServlet() == null) {
      throw new IllegalStateException("Failed to create Iceberg REST Catalog servlet");
    }

    ServletRegistrationBean<HttpServlet> registration =
        new ServletRegistrationBean<>(descriptor.getServlet(), "/" + servletPath + "/*");
    registration.setName("IcebergRESTCatalog");
    registration.setLoadOnStartup(1);

    return registration;
  }
}
