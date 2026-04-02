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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ServletServerBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.HMSCatalogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;

/**
 * Spring configuration for the Iceberg REST Catalog.
 */
@org.springframework.context.annotation.Configuration
public class IcebergCatalogConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergCatalogConfiguration.class);
  public static final String DEFAULT_SERVLET_PATH = "iceberg";
  public static final int DEFAULT_PORT = 8080;

  @Bean
  public Configuration hadoopConfiguration(ApplicationArguments args) {
    Configuration conf = MetastoreConf.newMetastoreConf();
    for (String arg : args.getSourceArgs()) {
      if (arg.startsWith("-D")) {
        String[] kv = arg.substring(2).split("=", 2);
        if (kv.length == 2) {
          conf.set(kv[0], kv[1]);
        }
      }
    }
    return conf;
  }

  @Bean
  public ServletRegistrationBean<?> restCatalogServlet(Configuration conf) {
    return createRestCatalogServlet(conf);
  }

  /**
   * Creates the REST Catalog servlet registration. Shared by production config and tests.
   */
  public static ServletRegistrationBean<?> createRestCatalogServlet(Configuration conf) {
    String servletPath = MetastoreConf.getVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
    if (servletPath == null || servletPath.isEmpty()) {
      servletPath = DEFAULT_SERVLET_PATH;
      MetastoreConf.setVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, servletPath);
    }

    int port = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
    if (port == 0) {
      port = DEFAULT_PORT;
      MetastoreConf.setLongVar(conf, ConfVars.CATALOG_SERVLET_PORT, port);
    }

    LOG.info("Creating REST Catalog servlet at /{}", servletPath);

    ServletServerBuilder.Descriptor descriptor = HMSCatalogFactory.createServlet(conf);
    if (descriptor == null || descriptor.getServlet() == null) {
      throw new IllegalStateException("Failed to create Iceberg REST Catalog servlet");
    }
    ServletRegistrationBean<?> registration =
        new ServletRegistrationBean(descriptor.getServlet(), "/" + servletPath + "/*");
    registration.setName("IcebergRESTCatalog");
    registration.setLoadOnStartup(1);

    return registration;
  }
}
