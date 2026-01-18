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

package org.apache.iceberg.rest.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.IPStackUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.registry.ServiceInstanceStateChangeListener;
import org.apache.hadoop.hive.registry.ServiceRegistry;
import org.apache.hadoop.hive.metastore.registry.impl.ZkRegistryBase;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * REST Catalog High Availability Registry.
 * Similar to HS2ActivePassiveHARegistry, provides ZooKeeper-based service discovery
 * and leader election for REST Catalog instances.
 */
public class RESTCatalogHARegistry extends ZkRegistryBase<RESTCatalogInstance> implements
    ServiceRegistry<RESTCatalogInstance>, RESTCatalogHAInstanceSet {
  
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalogHARegistry.class);
  static final String ACTIVE_ENDPOINT = "activeEndpoint";
  static final String PASSIVE_ENDPOINT = "passiveEndpoint";
  private static final String SASL_LOGIN_CONTEXT_NAME = "RESTCatalogHAZooKeeperClient";
  private static final String INSTANCE_PREFIX = "instance-";
  private static final String INSTANCE_GROUP = "instances";
  private static final String LEADER_LATCH_PATH = "/_LEADER";
  
  private LeaderLatch leaderLatch;
  private Map<LeaderLatchListener, ExecutorService> registeredListeners = new HashMap<>();
  private String latchPath;
  private ServiceRecord srv;
  private boolean isClient;
  private final String uniqueId;

  // There are 2 paths under which the instances get registered
  // 1) Standard path used by ZkRegistryBase where all instances register themselves
  // Secure: /restCatalogHA-sasl/instances/instance-0000000000
  // Unsecure: /restCatalogHA-unsecure/instances/instance-0000000000
  // 2) Leader latch path used for REST Catalog HA Active/Passive configuration
  // Secure: /restCatalogHA-sasl/_LEADER/xxxx-latch-0000000000
  // Unsecure: /restCatalogHA-unsecure/_LEADER/xxxx-latch-0000000000
  
  /**
   * Factory method to create REST Catalog HA Registry.
   * @param conf Configuration
   * @param isClient true if this is a client-side registry, false if server-side
   * @return RESTCatalogHARegistry instance
   */
  public static RESTCatalogHARegistry create(Configuration conf, boolean isClient) {
    String zkNameSpace = MetastoreConf.getVar(conf, ConfVars.REST_CATALOG_HA_REGISTRY_NAMESPACE);
    Preconditions.checkArgument(!StringUtils.isBlank(zkNameSpace),
        ConfVars.REST_CATALOG_HA_REGISTRY_NAMESPACE.getVarname() + " cannot be null or empty");
    String zkNameSpacePrefix = zkNameSpace + "-";
    return new RESTCatalogHARegistry(null, zkNameSpacePrefix, LEADER_LATCH_PATH,
        null, null, isClient ? SASL_LOGIN_CONTEXT_NAME : null, conf, isClient);
  }

  private RESTCatalogHARegistry(final String instanceName, final String zkNamespacePrefix,
      final String leaderLatchPath, final String krbPrincipal, final String krbKeytab,
      final String saslContextName, final Configuration conf, final boolean isClient) {
    super(instanceName, conf, null, zkNamespacePrefix, null, INSTANCE_PREFIX, INSTANCE_GROUP,
        saslContextName, krbPrincipal, krbKeytab, null);
    this.isClient = isClient;
    if (MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST) &&
        conf.get(ZkRegistryBase.UNIQUE_IDENTIFIER) != null) {
      this.uniqueId = conf.get(ZkRegistryBase.UNIQUE_IDENTIFIER);
    } else {
      this.uniqueId = UUID.randomUUID().toString();
    }
    this.latchPath = leaderLatchPath;
    this.leaderLatch = getNewLeaderLatchPath();
  }

  @Override
  public void start() throws IOException {
    super.start();
    if (!isClient) {
      this.srv = getNewServiceRecord();
      register();
      registerLeaderLatchListener(new RESTCatalogLeaderLatchListener(), null);
      try {
        // All participating instances use the same latch path, and curator randomly chooses one instance to be leader
        leaderLatch.start();
      } catch (Exception e) {
        throw new IOException(e);
      }
      LOG.info("Registered REST Catalog with ZK. service record: {}", srv);
    } else {
      populateCache();
      LOG.info("Populating instances cache for client");
    }
  }

  @Override
  protected void unregisterInternal() {
    super.unregisterInternal();
  }

  @Override
  public String register() throws IOException {
    updateEndpoint(srv, PASSIVE_ENDPOINT);
    return registerServiceRecord(srv, uniqueId);
  }

  @Override
  public void unregister() {
    if (leaderLatch != null) {
      try {
        leaderLatch.close();
      } catch (IllegalStateException e) {
        // Already closed, ignore
        LOG.debug("LeaderLatch already closed during unregister", e);
      } catch (Exception e) {
        LOG.warn("Error closing LeaderLatch during unregister", e);
      }
      leaderLatch = null;
    }
    unregisterInternal();
  }

  @Override
  public void updateRegistration(Iterable<Map.Entry<String, String>> attributes) throws IOException {
    throw new UnsupportedOperationException();
  }

  private void populateCache() throws IOException {
    PathChildrenCache pcc = ensureInstancesCache(0);
    populateCache(pcc, false);
  }

  @Override
  public ServiceInstanceSet<RESTCatalogInstance> getInstances(final String component, 
      final long clusterReadyTimeoutMs) throws IOException {
    throw new IOException("Not supported to get instances by component name");
  }

  private void addActiveEndpointToServiceRecord() throws IOException {
    addEndpointToServiceRecord(getNewServiceRecord(), ACTIVE_ENDPOINT);
  }

  private void addPassiveEndpointToServiceRecord() throws IOException {
    addEndpointToServiceRecord(getNewServiceRecord(), PASSIVE_ENDPOINT);
  }

  private void addEndpointToServiceRecord(final ServiceRecord srv, final String endpointName) 
      throws IOException {
    updateEndpoint(srv, endpointName);
    updateServiceRecord(srv, doCheckAcls, true);
  }

  private void updateEndpoint(final ServiceRecord srv, final String endpointName) {
    String instanceUri = srv.get(ConfVars.REST_CATALOG_INSTANCE_URI.getVarname());
    IPStackUtils.HostPort hostPort;
    if (instanceUri != null && !instanceUri.isEmpty()) {
      hostPort = IPStackUtils.getHostAndPort(instanceUri);
    } else {
      // Fallback: use hostname and port from configuration
      String hostname = ZkRegistryBase.hostname;
      int port = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
      if (port < 0) {
        port = 8080; // Default port
      }
      hostPort = new IPStackUtils.HostPort(hostname, port);
    }
    final String endpointHostname = hostPort.getHostname();
    final int endpointPort = hostPort.getPort();
    Endpoint urlEndpoint = RegistryTypeUtils.ipcEndpoint(endpointName, 
        new InetSocketAddress(endpointHostname, endpointPort));
    srv.addInternalEndpoint(urlEndpoint);
    LOG.info("Added {} endpoint to service record: {}", endpointName, urlEndpoint);
  }

  @Override
  public void stop() {
    if (leaderLatch != null) {
      try {
        leaderLatch.close();
      } catch (IllegalStateException e) {
        // Already closed, ignore
        LOG.debug("LeaderLatch already closed during stop", e);
      } catch (Exception e) {
        LOG.warn("Error closing LeaderLatch during stop", e);
      }
      leaderLatch = null;
    }
    super.stop();
  }

  @Override
  protected RESTCatalogInstance createServiceInstance(final ServiceRecord srv) throws IOException {
    Endpoint activeEndpoint = srv.getInternalEndpoint(ACTIVE_ENDPOINT);
    return new RESTCatalogInstance(srv, activeEndpoint != null ? ACTIVE_ENDPOINT : PASSIVE_ENDPOINT);
  }

  @Override
  public synchronized void registerStateChangeListener(
      final ServiceInstanceStateChangeListener<RESTCatalogInstance> listener) throws IOException {
    super.registerStateChangeListener(listener);
  }

  @Override
  protected String getZkPathUser(final Configuration conf) {
    return currentUser();
  }

  @Override
  public org.apache.hadoop.yarn.api.records.ApplicationId getApplicationId() throws IOException {
    throw new IOException("Not supported until REST Catalog runs as YARN application");
  }

  public boolean hasLeadership() {
    return leaderLatch != null && leaderLatch.hasLeadership();
  }

  public String getUniqueId() {
    return uniqueId;
  }

  /**
   * Returns a new instance of leader latch path but retains the same uniqueId.
   * @return new leader latch
   */
  private LeaderLatch getNewLeaderLatchPath() {
    return new LeaderLatch(zooKeeperClient, latchPath, uniqueId, 
        LeaderLatch.CloseMode.NOTIFY_LEADER);
  }

  private class RESTCatalogLeaderLatchListener implements LeaderLatchListener {
    // Leadership state changes happen inside synchronized methods in curator.
    // Do only lightweight actions in main-event handler thread.
    @Override
    public void isLeader() {
      // Only leader publishes instance uri as endpoint which will be used by clients
      try {
        if (!hasLeadership()) {
          LOG.info("isLeader notification received but hasLeadership returned false.. awaiting..");
          leaderLatch.await();
        }
        addActiveEndpointToServiceRecord();
        LOG.info("REST Catalog instance in ACTIVE mode. Service record: {}", srv);
      } catch (Exception e) {
        throw new RuntimeException("Unable to add active endpoint to service record", e);
      }
    }

    @Override
    public void notLeader() {
      try {
        if (hasLeadership()) {
          LOG.info("notLeader notification received but hasLeadership returned true.. awaiting..");
          leaderLatch.await();
        }
        addPassiveEndpointToServiceRecord();
        LOG.info("REST Catalog instance lost leadership. Switched to PASSIVE standby mode.");
      } catch (Exception e) {
        throw new RuntimeException("Unable to add passive endpoint to service record", e);
      }
    }
  }

  @Override
  public RESTCatalogInstance getLeader() {
    for (RESTCatalogInstance instance : getAll()) {
      if (instance.isLeader()) {
        return instance;
      }
    }
    return null;
  }

  @Override
  public Collection<RESTCatalogInstance> getAll() {
    return getAllInternal();
  }

  @Override
  public RESTCatalogInstance getInstance(final String instanceId) {
    for (RESTCatalogInstance instance : getAll()) {
      if (instance.getWorkerIdentity().equals(instanceId)) {
        return instance;
      }
    }
    return null;
  }

  @Override
  public Set<RESTCatalogInstance> getByHost(final String host) {
    return getByHostInternal(host);
  }

  @Override
  public int size() {
    return sizeInternal();
  }

  /**
   * Register leader latch listener for leadership change notifications.
   * @param latchListener listener
   * @param executorService event handler executor service
   */
  public void registerLeaderLatchListener(final LeaderLatchListener latchListener, 
      final ExecutorService executorService) {
    registeredListeners.put(latchListener, executorService);
    if (executorService == null) {
      leaderLatch.addListener(latchListener);
    } else {
      leaderLatch.addListener(latchListener, executorService);
    }
  }

  private Map<String, String> getConfsToPublish() {
    final Map<String, String> confsToPublish = new HashMap<>();
    // Instance URI
    String instanceUri = MetastoreConf.getVar(conf, ConfVars.REST_CATALOG_INSTANCE_URI);
    if (instanceUri == null || instanceUri.isEmpty()) {
      int port = MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT);
      if (port < 0) {
        port = 8080;
      }
      instanceUri = ZkRegistryBase.hostname + ":" + port;
    }
    confsToPublish.put(ConfVars.REST_CATALOG_INSTANCE_URI.getVarname(), instanceUri);
    confsToPublish.put(ZkRegistryBase.UNIQUE_IDENTIFIER, uniqueId);
    // REST Catalog path
    confsToPublish.put(ConfVars.ICEBERG_CATALOG_SERVLET_PATH.getVarname(),
        MetastoreConf.getVar(conf, ConfVars.ICEBERG_CATALOG_SERVLET_PATH));
    // Port
    confsToPublish.put(ConfVars.CATALOG_SERVLET_PORT.getVarname(),
        String.valueOf(MetastoreConf.getIntVar(conf, ConfVars.CATALOG_SERVLET_PORT)));
    return confsToPublish;
  }

  private ServiceRecord getNewServiceRecord() {
    ServiceRecord srv = new ServiceRecord();
    final Map<String, String> confsToPublish = getConfsToPublish();
    for (Map.Entry<String, String> entry : confsToPublish.entrySet()) {
      srv.set(entry.getKey(), entry.getValue());
    }
    return srv;
  }
}

