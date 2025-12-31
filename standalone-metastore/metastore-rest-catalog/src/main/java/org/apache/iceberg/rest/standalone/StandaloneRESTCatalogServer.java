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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.ServletServerBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.HMSCatalogFactory;
import org.apache.iceberg.rest.ha.RESTCatalogHARegistry;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Standalone REST Catalog Server with High Availability support.
 * Can be deployed independently from HMS and scaled independently.
 */
public class StandaloneRESTCatalogServer {
  private static final Logger LOG = LoggerFactory.getLogger(StandaloneRESTCatalogServer.class);
  
  private Server server;
  private Configuration conf;
  private RESTCatalogHARegistry haRegistry;
  private AtomicBoolean isLeader = new AtomicBoolean(false);
  private ExecutorService leaderActionsExecutorService;
  private LeaderLatchListener leaderLatchListener;
  
  public StandaloneRESTCatalogServer(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * Start the REST Catalog server.
   * @throws Exception if startup fails
   */
  public void start() throws Exception {
    // Check if HA is enabled
    boolean haEnabled = MetastoreConf.getBoolVar(conf, ConfVars.REST_CATALOG_HA_ENABLED);
    
    if (haEnabled) {
      LOG.info("Starting REST Catalog server with High Availability");
      startWithHA();
    } else {
      LOG.info("Starting REST Catalog server without HA");
      startWithoutHA();
    }
  }
  
  private void startWithHA() throws Exception {
    // Initialize HA Registry
    haRegistry = RESTCatalogHARegistry.create(conf, false);
    
    // Create leader latch listener
    leaderLatchListener = new RESTCatalogLeaderLatchListener(this);
    leaderActionsExecutorService = Executors.newSingleThreadExecutor(
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("REST-Catalog-Leader-Actions-%d")
            .build());
    
    // Register leader latch listener
    haRegistry.registerLeaderLatchListener(leaderLatchListener, leaderActionsExecutorService);
    
    // Start HA registry (registers in ZooKeeper)
    haRegistry.start();
    
    // Start the servlet server
    int restPort = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
    if (restPort < 0) {
      restPort = 8080; // Default port
    }
    
    ServletServerBuilder builder = new ServletServerBuilder(conf);
    ServletServerBuilder.Descriptor restCatalogDescriptor = 
        HMSCatalogFactory.createServlet(conf);
    
    if (restCatalogDescriptor == null) {
      throw new RuntimeException("Failed to create REST Catalog servlet");
    }
    
    builder.addServlet(restCatalogDescriptor);
    server = builder.start(LOG);
    
    if (server == null || !server.isStarted()) {
      throw new RuntimeException("Failed to start REST Catalog server");
    }
    
    LOG.info("REST Catalog server started (waiting for leadership) on port {}", restPort);
  }
  
  private void startWithoutHA() throws Exception {
    int restPort = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
    if (restPort < 0) {
      restPort = 8080;
    }
    
    String hmsUris = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
    if (hmsUris == null || hmsUris.isEmpty()) {
      throw new IllegalArgumentException("hive.metastore.uris must be configured");
    }
    
    LOG.info("Starting standalone REST Catalog server on port {}", restPort);
    LOG.info("Connecting to HMS at: {}", hmsUris);
    
    ServletServerBuilder builder = new ServletServerBuilder(conf);
    ServletServerBuilder.Descriptor restCatalogDescriptor = 
        HMSCatalogFactory.createServlet(conf);
    
    if (restCatalogDescriptor == null) {
      throw new RuntimeException("Failed to create REST Catalog servlet");
    }
    
    builder.addServlet(restCatalogDescriptor);
    server = builder.start(LOG);
    
    if (server == null || !server.isStarted()) {
      throw new RuntimeException("Failed to start REST Catalog server");
    }
    
    LOG.info("REST Catalog server started successfully on port {}", restPort);
  }
  
  private class RESTCatalogLeaderLatchListener implements LeaderLatchListener {
    private final StandaloneRESTCatalogServer server;
    
    RESTCatalogLeaderLatchListener(StandaloneRESTCatalogServer server) {
      this.server = server;
    }
    
    @Override
    public void isLeader() {
      LOG.info("REST Catalog instance {} became the LEADER", 
          haRegistry.getUniqueId());
      server.isLeader.set(true);
      // Server is now active and accepting connections
    }
    
    @Override
    public void notLeader() {
      LOG.info("REST Catalog instance {} LOST LEADERSHIP", 
          haRegistry.getUniqueId());
      server.isLeader.set(false);
      // Optionally: stop accepting new connections, drain existing ones
      // For now, we'll continue serving but mark as passive
    }
  }
  
  /**
   * Stop the REST Catalog server.
   * @throws Exception if shutdown fails
   */
  public void stop() throws Exception {
    if (haRegistry != null) {
      haRegistry.unregister();
      haRegistry.stop();
    }
    if (leaderActionsExecutorService != null) {
      leaderActionsExecutorService.shutdown();
    }
    if (server != null) {
      server.stop();
      LOG.info("REST Catalog server stopped");
    }
  }
  
  /**
   * Check if this instance is the leader.
   * @return true if leader, false otherwise
   */
  public boolean isLeader() {
    return isLeader.get();
  }
  
  /**
   * Get the server instance.
   * @return Jetty server instance
   */
  public Server getServer() {
    return server;
  }
  
  /**
   * Main method for standalone deployment.
   * @param args command line arguments
   */
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    
    // Load configuration from files
    conf.addResource("hive-site.xml");
    conf.addResource("core-site.xml");
    
    // Override with system properties
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("hive.") || key.startsWith("metastore.")) {
        conf.set(key, System.getProperty(key));
      }
    }
    
    StandaloneRESTCatalogServer server = new StandaloneRESTCatalogServer(conf);
    
    // Add shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        server.stop();
      } catch (Exception e) {
        LOG.error("Error stopping server", e);
      }
    }));
    
    server.start();
    
    // Keep running
    if (server.getServer() != null) {
      server.getServer().join();
    }
  }
}

