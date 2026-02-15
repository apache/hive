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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ServletServerBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.HMSCatalogFactory;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Standalone REST Catalog Server.
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
public class StandaloneRESTCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneRESTCatalogServer.class);
  
  private final Configuration conf;
  private Server server;
  private int port;
  
  public StandaloneRESTCatalogServer(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * Starts the standalone REST Catalog server.
   */
  public void start() {
    // Validate required configuration
    String thriftUris = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
    if (thriftUris == null || thriftUris.isEmpty()) {
      throw new IllegalArgumentException("metastore.thrift.uris must be configured to connect to HMS");
    }
    
    int servletPort = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
    String servletPath = MetastoreConf.getVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
    
    if (servletPath == null || servletPath.isEmpty()) {
      servletPath = "iceberg"; // Default path
      MetastoreConf.setVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH, servletPath);
    }
    
    LOG.info("Starting Standalone REST Catalog Server");
    LOG.info("  HMS Thrift URIs: {}", thriftUris);
    LOG.info("  Servlet Port: {}", servletPort);
    LOG.info("  Servlet Path: /{}", servletPath);
    
    // Create servlet using factory
    ServletServerBuilder.Descriptor catalogDescriptor = HMSCatalogFactory.createServlet(conf);
    if (catalogDescriptor == null) {
      throw new IllegalStateException("Failed to create REST Catalog servlet. " +
          "Check that metastore.catalog.servlet.port and metastore.iceberg.catalog.servlet.path are configured.");
    }
    
    // Create health check servlet
    HealthCheckServlet healthServlet = new HealthCheckServlet();
    
    // Build and start server
    ServletServerBuilder builder = new ServletServerBuilder(conf);
    builder.addServlet(catalogDescriptor);
    builder.addServlet(servletPort, "health", healthServlet);
    
    server = builder.start(LOG);
    if (server == null || !server.isStarted()) {
      // Server failed to start - likely a port conflict
      throw new IllegalStateException(String.format(
          "Failed to start REST Catalog server on port %d. Port may already be in use. ", servletPort));
    }
    
    // Get actual port (may be auto-assigned)
    port = catalogDescriptor.getPort();
    LOG.info("Standalone REST Catalog Server started successfully on port {}", port);
    LOG.info("  REST Catalog endpoint: http://localhost:{}/{}", port, servletPath);
    LOG.info("  Health check endpoint: http://localhost:{}/health", port);
  }
  
  /**
   * Stops the server.
   */
  public void stop() {
    if (server != null && server.isStarted()) {
      try {
        LOG.info("Stopping Standalone REST Catalog Server");
        server.stop();
        server.join();
        LOG.info("Standalone REST Catalog Server stopped");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Server stop interrupted", e);
      } catch (Exception e) {
        LOG.error("Error stopping server", e);
      }
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
    String servletPath = MetastoreConf.getVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH);
    if (servletPath == null || servletPath.isEmpty()) {
      servletPath = "iceberg";
    }
    return "http://localhost:" + port + "/" + servletPath;
  }
  
  /**
   * Simple health check servlet for Kubernetes readiness/liveness probes.
   */
  private static final class HealthCheckServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
      try {
        resp.setContentType("application/json");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.getWriter().println("{\"status\":\"healthy\"}");
      } catch (IOException e) {
        LOG.warn("Failed to write health check response", e);
      }
    }
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
    
    StandaloneRESTCatalogServer server = new StandaloneRESTCatalogServer(conf);
    
    // Add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("Shutdown hook triggered");
      server.stop();
    }));
    
    try {
      server.start();
      LOG.info("Server running. Press Ctrl+C to stop.");
      
      // Keep server running
      server.server.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Server stop interrupted", e);
    } catch (Exception e) {
      LOG.error("Failed to start server", e);
      System.exit(1);
    }
  }
}
