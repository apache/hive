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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.event.EventListener;

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
  
  private final Configuration conf;
  private String restEndpoint;
  private int port;
  
  /**
   * Constructor that accepts Configuration.
   * Standard Hive approach - caller controls Configuration creation.
   */
  public StandaloneRESTCatalogServer(Configuration conf) {
    this.conf = conf;

    // Validate required configuration
    String thriftUris = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
    if (thriftUris == null || thriftUris.isEmpty()) {
      throw new IllegalArgumentException("metastore.thrift.uris must be configured to connect to HMS");
    }
    
    LOG.info("Hadoop Configuration initialized");
    LOG.info("  HMS Thrift URIs: {}", thriftUris);
    LOG.info("  Warehouse: {}", MetastoreConf.getVar(conf, ConfVars.WAREHOUSE));
  }
  
  /**
   * Updates port and restEndpoint with the actual server port once the web server has started.
   * Handles RANDOM_PORT (tests) and server.port=0 where the real port differs from config.
   */
  @EventListener
  public void onWebServerInitialized(WebServerInitializedEvent event) {
    int actualPort = event.getWebServer().getPort();
    if (actualPort > 0) {
      this.port = actualPort;
      String servletPath = MetastoreConf.getVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
      if (servletPath == null || servletPath.isEmpty()) {
        servletPath = "iceberg";
      }
      this.restEndpoint = "http://localhost:" + actualPort + "/" + servletPath;
      LOG.info("REST endpoint set to actual server port: {}", restEndpoint);
    }
  }

  /**
   * Gets the port the server is listening on.
   * @return the port number
   */
  @VisibleForTesting
  public int getPort() {
    return port;
  }

  /**
   * Gets the REST Catalog endpoint URL.
   * @return the endpoint URL
   */
  public String getRestEndpoint() {
    return restEndpoint;
  }
  
  /**
   * Main method for running as a standalone application.
   * @param args command line arguments
   */
  public static void main(String[] args) {
    Configuration conf = MetastoreConf.newMetastoreConf();
    
    // Load configuration from command line args or environment
    // Format: -Dkey=value or use system properties
    for (String arg : args) {
      if (arg.startsWith("-D")) {
        String[] kv = arg.substring(2).split("=", 2);
        if (kv.length == 2) {
          conf.set(kv[0], kv[1]);
        }
      }
    }
    
    // Sync port from MetastoreConf to Spring's Environment so server.port uses it
    int port = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
    if (port > 0) {
      System.setProperty(ConfVars.CATALOG_SERVLET_PORT.getVarname(), String.valueOf(port));
    }
    
    StandaloneRESTCatalogServer server = new StandaloneRESTCatalogServer(conf);

    // Start Spring Boot with pre-configured beans
    SpringApplication app = new SpringApplication(StandaloneRESTCatalogServer.class, IcebergCatalogConfiguration.class);
    app.addInitializers(ctx -> {
      ctx.getBeanFactory().registerSingleton("hadoopConfiguration", conf);
      ctx.getBeanFactory().registerSingleton("standaloneRESTCatalogServer", server);
    });
    
    app.run(args);
    
    LOG.info("Standalone REST Catalog Server started successfully");
    LOG.info("Server running. Press Ctrl+C to stop.");
    
    // Spring Boot's graceful shutdown will handle cleanup automatically
  }
}
