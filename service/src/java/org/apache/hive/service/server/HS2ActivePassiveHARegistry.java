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

package org.apache.hive.service.server;

import static org.apache.hive.service.server.HiveServer2.INSTANCE_URI_CONFIG;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.registry.ServiceRegistry;
import org.apache.hadoop.hive.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.registry.ServiceInstanceStateChangeListener;
import org.apache.hadoop.hive.registry.impl.ZkRegistryBase;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hive.service.ServiceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class HS2ActivePassiveHARegistry extends ZkRegistryBase<HiveServer2Instance> implements
  ServiceRegistry<HiveServer2Instance>, HiveServer2HAInstanceSet, HiveServer2.FailoverHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HS2ActivePassiveHARegistry.class);
  static final String ACTIVE_ENDPOINT = "activeEndpoint";
  static final String PASSIVE_ENDPOINT = "passiveEndpoint";
  private static final String SASL_LOGIN_CONTEXT_NAME = "HS2ActivePassiveHAZooKeeperClient";
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
  // 1) Standard path used by ZkRegistryBase where all instances register themselves (also stores metadata)
  // Secure: /hs2ActivePassiveHA-sasl/instances/instance-0000000000
  // Unsecure: /hs2ActivePassiveHA-unsecure/instances/instance-0000000000
  // 2) Leader latch path used for HS2 HA Active/Passive configuration where all instances register under _LEADER
  //    path but only one among them is the leader
  // Secure: /hs2ActivePassiveHA-sasl/_LEADER/xxxx-latch-0000000000
  // Unsecure: /hs2ActivePassiveHA-unsecure/_LEADER/xxxx-latch-0000000000
  static HS2ActivePassiveHARegistry create(Configuration conf, boolean isClient) {
    String zkNameSpace = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_REGISTRY_NAMESPACE);
    Preconditions.checkArgument(!StringUtils.isBlank(zkNameSpace),
      HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_REGISTRY_NAMESPACE.varname + " cannot be null or empty");
    String principal = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    String keytab = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    String zkNameSpacePrefix = zkNameSpace + "-";
    return new HS2ActivePassiveHARegistry(null, zkNameSpacePrefix, LEADER_LATCH_PATH, principal, keytab,
      isClient ? null : SASL_LOGIN_CONTEXT_NAME, conf, isClient);
  }

  private HS2ActivePassiveHARegistry(final String instanceName, final String zkNamespacePrefix,
    final String leaderLatchPath,
    final String krbPrincipal, final String krbKeytab, final String saslContextName, final Configuration conf,
    final boolean isClient) {
    super(instanceName, conf, null, zkNamespacePrefix, null, INSTANCE_PREFIX, INSTANCE_GROUP,
      saslContextName, krbPrincipal, krbKeytab, null);
    this.isClient = isClient;
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST) &&
      conf.get(ZkRegistryBase.UNIQUE_IDENTIFIER) != null) {
      this.uniqueId = conf.get(ZkRegistryBase.UNIQUE_IDENTIFIER);
    } else {
      this.uniqueId = UNIQUE_ID.toString();
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
      registerLeaderLatchListener(new HS2LeaderLatchListener(), null);
      try {
        // all participating instances uses the same latch path, and curator randomly chooses one instance to be leader
        // which can be verified via leaderLatch.hasLeadership()
        leaderLatch.start();
      } catch (Exception e) {
        throw new IOException(e);
      }
      LOG.info("Registered HS2 with ZK. service record: {}", srv);
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
    CloseableUtils.closeQuietly(leaderLatch);
    unregisterInternal();
  }

  private void populateCache() throws IOException {
    PathChildrenCache pcc = ensureInstancesCache(0);
    populateCache(pcc, false);
  }

  @Override
  public ServiceInstanceSet<HiveServer2Instance> getInstances(final String component, final long clusterReadyTimeoutMs)
    throws IOException {
    throw new IOException("Not supported to get instances by component name");
  }

  private void addActiveEndpointToServiceRecord() throws IOException {
    addEndpointToServiceRecord(getNewServiceRecord(), ACTIVE_ENDPOINT);
  }

  private void addPassiveEndpointToServiceRecord() throws IOException {
    addEndpointToServiceRecord(getNewServiceRecord(), PASSIVE_ENDPOINT);
  }

  private void addEndpointToServiceRecord(
      final ServiceRecord srv, final String endpointName) throws IOException {
    updateEndpoint(srv, endpointName);
    updateServiceRecord(srv, doCheckAcls, true);
  }

  private void updateEndpoint(final ServiceRecord srv, final String endpointName) {
    final String instanceUri = srv.get(INSTANCE_URI_CONFIG);
    final String[] tokens = instanceUri.split(":");
    final String hostname = tokens[0];
    final int port = Integer.parseInt(tokens[1]);
    Endpoint urlEndpoint = RegistryTypeUtils.ipcEndpoint(endpointName, new InetSocketAddress(hostname, port));
    srv.addInternalEndpoint(urlEndpoint);
    LOG.info("Added {} endpoint to service record", urlEndpoint);
  }

  @Override
  public void stop() {
    CloseableUtils.closeQuietly(leaderLatch);
    super.stop();
  }

  @Override
  protected HiveServer2Instance createServiceInstance(final ServiceRecord srv) throws IOException {
    Endpoint activeEndpoint = srv.getInternalEndpoint(HS2ActivePassiveHARegistry.ACTIVE_ENDPOINT);
    return new HiveServer2Instance(srv, activeEndpoint != null ? ACTIVE_ENDPOINT : PASSIVE_ENDPOINT);
  }

  @Override
  public synchronized void registerStateChangeListener(
    final ServiceInstanceStateChangeListener<HiveServer2Instance> listener)
    throws IOException {
    super.registerStateChangeListener(listener);
  }

  @Override
  public ApplicationId getApplicationId() throws IOException {
    throw new IOException("Not supported until HS2 runs as YARN application");
  }

  @Override
  protected String getZkPathUser(final Configuration conf) {
    return currentUser();
  }

  private boolean hasLeadership() {
    return leaderLatch.hasLeadership();
  }

  @Override
  public void failover() throws Exception {
    if (hasLeadership()) {
      LOG.info("Failover request received for HS2 instance: {}. Restarting leader latch..", uniqueId);
      leaderLatch.close(LeaderLatch.CloseMode.NOTIFY_LEADER);
      leaderLatch = getNewLeaderLatchPath();
      // re-attach all registered listeners
      for (Map.Entry<LeaderLatchListener, ExecutorService> registeredListener : registeredListeners.entrySet()) {
        if (registeredListener.getValue() == null) {
          leaderLatch.addListener(registeredListener.getKey());
        } else {
          leaderLatch.addListener(registeredListener.getKey(), registeredListener.getValue());
        }
      }
      leaderLatch.start();
      LOG.info("Failover complete. Leader latch restarted successfully. New leader: {}",
        leaderLatch.getLeader().getId());
    } else {
      LOG.warn("Failover request received for HS2 instance: {} that is not leader. Skipping..", uniqueId);
    }
  }

  /**
   * Returns a new instance of leader latch path but retains the same uniqueId. This is only used when HS2 startsup or
   * when a manual failover is triggered (in which case uniqueId will still remain as the instance has not restarted)
   *
   * @return - new leader latch
   */
  private LeaderLatch getNewLeaderLatchPath() {
    return new LeaderLatch(zooKeeperClient, latchPath, uniqueId, LeaderLatch.CloseMode.NOTIFY_LEADER);
  }

  private class HS2LeaderLatchListener implements LeaderLatchListener {

    // leadership state changes and sending out notifications to listener happens inside synchronous method in curator.
    // Do only lightweight actions in main-event handler thread. Time consuming operations are handled via separate
    // executor service registered via registerLeaderLatchListener().
    @Override
    public void isLeader() {
      // only leader publishes instance uri as endpoint which will be used by clients to make connections to HS2 via
      // service discovery.
      try {
        if (!hasLeadership()) {
          LOG.info("isLeader notification received but hasLeadership returned false.. awaiting..");
          leaderLatch.await();
        }
        addActiveEndpointToServiceRecord();
        LOG.info("HS2 instance in ACTIVE mode. Service record: {}", srv);
      } catch (Exception e) {
        throw new ServiceException("Unable to add active endpoint to service record", e);
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
        LOG.info("HS2 instance lost leadership. Switched to PASSIVE standby mode. Service record: {}", srv);
      } catch (Exception e) {
        throw new ServiceException("Unable to add passive endpoint to service record", e);
      }
    }
  }

  @Override
  public HiveServer2Instance getLeader() {
    for (HiveServer2Instance hs2Instance : getAll()) {
      if (hs2Instance.isLeader()) {
        return hs2Instance;
      }
    }
    return null;
  }

  @Override
  public Collection<HiveServer2Instance> getAll() {
    return getAllInternal();
  }

  @Override
  public HiveServer2Instance getInstance(final String instanceId) {
    for (HiveServer2Instance hs2Instance : getAll()) {
      if (hs2Instance.getWorkerIdentity().equals(instanceId)) {
        return hs2Instance;
      }
    }
    return null;
  }

  @Override
  public Set<HiveServer2Instance> getByHost(final String host) {
    return getByHostInternal(host);
  }

  @Override
  public int size() {
    return sizeInternal();
  }

  /**
   * If leadership related notifications is desired, use this method to register leader latch listener.
   *
   * @param latchListener   - listener
   * @param executorService - event handler executor service
   */
  void registerLeaderLatchListener(final LeaderLatchListener latchListener, final ExecutorService executorService) {
    registeredListeners.put(latchListener, executorService);
    if (executorService == null) {
      leaderLatch.addListener(latchListener);
    } else {
      leaderLatch.addListener(latchListener, executorService);
    }
  }

  private Map<String, String> getConfsToPublish() {
    final Map<String, String> confsToPublish = new HashMap<>();
    // Hostname
    confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname,
      conf.get(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname));
    // Web port
    confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT.varname,
      conf.get(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT.varname));
    // Hostname:port
    confsToPublish.put(INSTANCE_URI_CONFIG, conf.get(INSTANCE_URI_CONFIG));
    confsToPublish.put(UNIQUE_IDENTIFIER, uniqueId);
    // Transport mode
    confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname,
      conf.get(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname));
    // Transport specific confs
    if (HiveServer2.isHTTPTransportMode(conf)) {
      confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT.varname,
        conf.get(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT.varname));
      confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname,
        conf.get(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PATH.varname));
    } else {
      confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname,
        conf.get(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT.varname));
      confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP.varname,
        conf.get(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP.varname));
    }
    // Auth specific confs
    confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname,
      conf.get(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname));
    if (HiveServer2.isKerberosAuthMode(conf)) {
      confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname,
        conf.get(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL.varname));
    }
    // SSL conf
    confsToPublish.put(HiveConf.ConfVars.HIVE_SERVER2_USE_SSL.varname,
      conf.get(HiveConf.ConfVars.HIVE_SERVER2_USE_SSL.varname));
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
