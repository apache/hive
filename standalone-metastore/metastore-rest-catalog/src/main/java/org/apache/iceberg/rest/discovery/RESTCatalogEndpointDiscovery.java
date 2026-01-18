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

package org.apache.iceberg.rest.discovery;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.iceberg.rest.ha.RESTCatalogHARegistry;
import org.apache.iceberg.rest.ha.RESTCatalogHARegistryHelper;
import org.apache.iceberg.rest.ha.RESTCatalogInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST Catalog Endpoint Discovery Helper.
 * Discovers REST Catalog server endpoints via ZooKeeper for HA setups.
 * Similar to how JDBC clients discover HiveServer2 instances.
 * 
 * This is NOT an HMS client - it's a discovery utility that returns endpoint URLs.
 * Use the discovered endpoint with HiveRESTCatalogClient or Iceberg RESTCatalog.
 * 
 * Usage:
 * <pre>
 *   Configuration conf = new Configuration();
 *   conf.set("hive.metastore.rest.catalog.ha.enabled", "true");
 *   conf.set("hive.zookeeper.quorum", "zk1:2181,zk2:2181");
 *   
 *   RESTCatalogEndpointDiscovery discovery = new RESTCatalogEndpointDiscovery(conf);
 *   String endpoint = discovery.getEndpoint();
 *   // Use endpoint to connect to REST Catalog via HiveRESTCatalogClient or RESTCatalog
 * </pre>
 */
public class RESTCatalogEndpointDiscovery {
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogEndpointDiscovery.class);
  
  private RESTCatalogHARegistry registry;
  private Configuration conf;
  private boolean haMode;
  
  public RESTCatalogEndpointDiscovery(Configuration conf) throws IOException {
    this.conf = conf;
    this.haMode = MetastoreConf.getBoolVar(conf, ConfVars.REST_CATALOG_HA_ENABLED);
    
    if (haMode) {
      this.registry = RESTCatalogHARegistryHelper.getRegistry(conf);
    }
  }
  
  /**
   * Get the REST Catalog endpoint URL.
   * In HA mode: returns leader's endpoint (active-passive) or random instance (active-active)
   * In non-HA mode: returns configured endpoint or discovers from HMS
   * 
   * @return REST Catalog endpoint URL
   * @throws IOException if discovery fails
   */
  public String getEndpoint() throws IOException {
    if (!haMode) {
      // Non-HA mode: use direct configuration
      String uri = MetastoreConf.getVar(conf, ConfVars.REST_CATALOG_INSTANCE_URI);
      if (uri != null && !uri.isEmpty()) {
        return "http://" + uri + "/iceberg";
      }
      // Fallback to HMS URIs discovery
      return discoverFromHMS();
    }
    
    // HA mode: discover from ZooKeeper
    Collection<RESTCatalogInstance> instances = registry.getAll();
    
    if (instances.isEmpty()) {
      throw new IOException("No REST Catalog instances found in ZooKeeper");
    }
    
    String haModeStr = MetastoreConf.getVar(conf, ConfVars.REST_CATALOG_HA_MODE);
    boolean isActivePassive = "active-passive".equalsIgnoreCase(haModeStr);
    
    if (isActivePassive) {
      // Active-Passive: return leader
      RESTCatalogInstance leader = registry.getLeader();
      if (leader != null) {
        LOG.info("Connecting to REST Catalog leader: {}", leader.getRestEndpoint());
        return leader.getRestEndpoint();
      } else {
        throw new IOException("No REST Catalog leader found");
      }
    } else {
      // Active-Active: return random instance
      List<RESTCatalogInstance> activeInstances = instances.stream()
          .filter(RESTCatalogInstance::isActive)
          .collect(Collectors.toList());
      
      if (activeInstances.isEmpty()) {
        // Fallback to all instances if none marked as active
        activeInstances = instances.stream().collect(Collectors.toList());
      }
      
      if (activeInstances.isEmpty()) {
        throw new IOException("No active REST Catalog instances found");
      }
      
      RESTCatalogInstance instance = activeInstances.get(
          new Random().nextInt(activeInstances.size()));
      LOG.info("Connecting to REST Catalog instance: {}", instance.getRestEndpoint());
      return instance.getRestEndpoint();
    }
  }
  
  /**
   * Get all available REST Catalog instances.
   * @return collection of instances
   * @throws IOException if discovery fails
   */
  public Collection<RESTCatalogInstance> getAllInstances() throws IOException {
    if (!haMode) {
      throw new IOException("HA mode is not enabled");
    }
    return registry.getAll();
  }
  
  /**
   * Get the leader instance (active-passive mode only).
   * @return leader instance or null if not found
   * @throws IOException if discovery fails
   */
  public RESTCatalogInstance getLeader() throws IOException {
    if (!haMode) {
      throw new IOException("HA mode is not enabled");
    }
    return registry.getLeader();
  }
  
  private String discoverFromHMS() {
    // Similar to how HMS clients discover HMS instances
    String hmsUris = MetastoreConf.getVar(conf, ConfVars.THRIFT_URIS);
    if (hmsUris == null || hmsUris.isEmpty()) {
      throw new RuntimeException("Neither REST Catalog HA nor HMS URIs are configured");
    }
    // Parse and return REST Catalog endpoint
    // This assumes REST Catalog runs on same hosts as HMS
    String[] uris = hmsUris.split(",");
    String firstUri = uris[0].trim();
    // Convert thrift://host:9083 to http://host:8080/iceberg
    String hostPort = firstUri.replace("thrift://", "");
    String[] parts = hostPort.split(":");
    String host = parts[0];
    // Default REST Catalog port
    int restPort = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
    if (restPort < 0) {
      restPort = 8080;
    }
    return "http://" + host + ":" + restPort + "/iceberg";
  }
  
  /**
   * Close the discovery helper and release resources.
   * @throws IOException if close fails
   */
  public void close() throws IOException {
    if (registry != null) {
      registry.stop();
    }
  }
}

