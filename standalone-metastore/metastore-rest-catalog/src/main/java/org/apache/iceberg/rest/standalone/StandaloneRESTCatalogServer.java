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

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;

/**
 * Standalone REST Catalog Server with Spring Boot.
 *
 * <p>This server runs independently of HMS and provides a REST API for Iceberg catalog operations.
 * It connects to an external HMS instance via Thrift.
 *
 * <p>Designed for Kubernetes deployment with load balancer/API gateway in front:
 * <pre>
 *   Client → Load Balancer/API Gateway → StandaloneRESTCatalogServer → HMS
 * </pre>
 *
 * <p>Multiple instances can run behind a Kubernetes Service for load balancing.
 */
@SpringBootApplication(exclude = DataSourceAutoConfiguration.class)
public class StandaloneRESTCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneRESTCatalogServer.class);

  /**
   * Main method for running as a standalone application.
   * @param args command line arguments (-Dkey=value for configuration)
   */
  public static void main(String[] args) {
    // Apply -D args to system properties so application.yml and Configuration bean pick them up
    for (String arg : args) {
      if (arg.startsWith("-D")) {
        String[] kv = arg.substring(2).split("=", 2);
        if (kv.length == 2) {
          System.setProperty(kv[0], kv[1]);
        }
      }
    }
    // Sync port from MetastoreConf to Spring's server.port if not already set
    if (System.getProperty(ConfVars.CATALOG_SERVLET_PORT.getVarname()) == null) {
      int port = MetastoreConf.getIntVar(MetastoreConf.newMetastoreConf(), ConfVars.CATALOG_SERVLET_PORT);
      if (port > 0) {
        System.setProperty(ConfVars.CATALOG_SERVLET_PORT.getVarname(), String.valueOf(port));
      }
    }

    SpringApplication.run(StandaloneRESTCatalogServer.class, args);

    LOG.info("Standalone REST Catalog Server started successfully");
    LOG.info("Server running. Press Ctrl+C to stop.");
  }
}
