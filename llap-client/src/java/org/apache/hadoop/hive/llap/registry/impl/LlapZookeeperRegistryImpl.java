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

package org.apache.hadoop.hive.llap.registry.impl;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.ServiceRegistry;
import org.apache.hadoop.hive.registry.impl.ServiceInstanceBase;
import org.apache.hadoop.hive.registry.impl.ZkRegistryBase;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils.ServiceRecordMarshal;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapZookeeperRegistryImpl
    extends ZkRegistryBase<LlapServiceInstance> implements ServiceRegistry<LlapServiceInstance> {
  private static final Logger LOG = LoggerFactory.getLogger(LlapZookeeperRegistryImpl.class);

  /**
   * IPC endpoint names.
   */
  private static final String IPC_SERVICES = "services";
  private static final String IPC_MNG = "llapmng";
  private static final String IPC_SHUFFLE = "shuffle";
  private static final String IPC_LLAP = "llap";
  private static final String IPC_OUTPUTFORMAT = "llapoutputformat";
  private static final String IPC_EXTERNAL_LLAP = "externalllap";
  private final static String NAMESPACE_PREFIX = "llap-";
  private static final String SLOT_PREFIX = "slot-";
  private static final String SASL_LOGIN_CONTEXT_NAME = "LlapZooKeeperClient";
  private static final String CONFIG_CHANGE_PATH = "config-change";
  private static final String CONFIG_CHANGE_NODE = "window-end";


  private SlotZnode slotZnode;
  private ServiceRecord daemonZkRecord;

  // to be used by clients of ServiceRegistry TODO: this is unnecessary
  private DynamicServiceInstanceSet instances;

  private DistributedAtomicLong lockWindowEnd;

  public LlapZookeeperRegistryImpl(String instanceName, Configuration conf) {
    super(instanceName, conf,
        HiveConf.getVar(conf, ConfVars.LLAP_ZK_REGISTRY_NAMESPACE), NAMESPACE_PREFIX,
        USER_SCOPE_PATH_PREFIX, WORKER_PREFIX, WORKER_GROUP,
        LlapProxy.isDaemon() ? SASL_LOGIN_CONTEXT_NAME : null,
        HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_PRINCIPAL),
        HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_KEYTAB_FILE),
        ConfVars.LLAP_VALIDATE_ACLS);
    LOG.info("Llap Zookeeper Registry is enabled with registryid: " + instanceName);
  }

  public Endpoint getRpcEndpoint() {
    final int rpcPort = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_RPC_PORT);
    return RegistryTypeUtils.ipcEndpoint(IPC_LLAP, new InetSocketAddress(hostname, rpcPort));
  }

  public Endpoint getShuffleEndpoint() {
    final int shufflePort = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_YARN_SHUFFLE_PORT);
    // HTTP today, but might not be
    return RegistryTypeUtils.inetAddrEndpoint(IPC_SHUFFLE, ProtocolTypes.PROTOCOL_TCP, hostname,
        shufflePort);
  }

  public Endpoint getServicesEndpoint() {
    final int servicePort = HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_WEB_PORT);
    final boolean isSSL = HiveConf.getBoolVar(conf, ConfVars.LLAP_DAEMON_WEB_SSL);
    final String scheme = isSSL ? "https" : "http";
    final URL serviceURL;
    try {
      serviceURL = new URL(scheme, hostname, servicePort, "");
      return RegistryTypeUtils.webEndpoint(IPC_SERVICES, serviceURL.toURI());
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    } catch (URISyntaxException e) {
      throw new RuntimeException("llap service URI for " + hostname + " is invalid", e);
    }
  }

  public Endpoint getMngEndpoint() {
    return RegistryTypeUtils.ipcEndpoint(IPC_MNG, new InetSocketAddress(hostname,
        HiveConf.getIntVar(conf, ConfVars.LLAP_MANAGEMENT_RPC_PORT)));
  }

  public Endpoint getOutputFormatEndpoint() {
    return RegistryTypeUtils.ipcEndpoint(IPC_OUTPUTFORMAT, new InetSocketAddress(hostname,
        HiveConf.getIntVar(conf, ConfVars.LLAP_DAEMON_OUTPUT_SERVICE_PORT)));
  }

  private Endpoint getExternalRpcEndpoint() {
    int port = HiveConf.getIntVar(conf, ConfVars.LLAP_EXTERNAL_CLIENT_CLOUD_RPC_PORT);
    String host = LlapUtil.getPublicHostname();
    return RegistryTypeUtils.ipcEndpoint(IPC_EXTERNAL_LLAP, new InetSocketAddress(host, port));
  }

  @Override
  public String register() throws IOException {
    daemonZkRecord = new ServiceRecord();
    Endpoint rpcEndpoint = getRpcEndpoint();
    daemonZkRecord.addInternalEndpoint(rpcEndpoint);
    daemonZkRecord.addInternalEndpoint(getMngEndpoint());
    daemonZkRecord.addInternalEndpoint(getShuffleEndpoint());
    daemonZkRecord.addExternalEndpoint(getServicesEndpoint());
    daemonZkRecord.addInternalEndpoint(getOutputFormatEndpoint());
    Endpoint externalRpcEndpoint = null;
    if (LlapUtil.isCloudDeployment(conf)) {
      externalRpcEndpoint = getExternalRpcEndpoint();
      daemonZkRecord.addExternalEndpoint(externalRpcEndpoint);
    }

    populateConfigValues(this.conf);
    Map<String, String> capacityValues = new HashMap<>(2);
    capacityValues.put(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS,
            HiveConf.getVarWithoutType(conf, ConfVars.LLAP_DAEMON_NUM_EXECUTORS));
    capacityValues.put(LlapRegistryService.LLAP_DAEMON_TASK_SCHEDULER_ENABLED_WAIT_QUEUE_SIZE,
            HiveConf.getVarWithoutType(conf, ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE));
    populateConfigValues(capacityValues.entrySet());

    String uniqueId = UNIQUE_ID.toString();
    long znodeCreationTimeout = 120;

    /* When no LLAP executors are running, the getInstances()/ llapRegistryService.getInstances() method results in
     InvalidACL exception when Hive tries to evict the cache in DROP DATABASE/TABLE

     As ContainerManager of zookeeper makes sure to cleanup znodes periodically, once query-coordinator zookeeper
     client session is terminated,entire path will get deleted sooner or later. This results in InvalidACL exception

     PersistentNode created on server will be preserved through restarts of query executor and makes sure
     proactive eviction call of DROP DATABASE/ DROP TABLE go through
     successfully. */
    ensurePersistentNodePath(daemonZkRecord);
    initializeWithoutRegisteringInternal();
    // Create a znode under the rootNamespace parent for this instance of the server
    try {
      slotZnode = new SlotZnode(
          zooKeeperClient, workersPath, SLOT_PREFIX, WORKER_PREFIX, uniqueId);
      if (!slotZnode.start(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception(
            "Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
      }
    } catch (Exception e) {
      LOG.error("Unable to create a znode for this server instance", e);
      CloseableUtils.closeQuietly(slotZnode);
      super.stop();
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }

    registerServiceRecord(daemonZkRecord, uniqueId);
    if (LlapUtil.isCloudDeployment(conf)) {
      LOG.info("Registered node. Created a znode on ZooKeeper for LLAP instance: rpc: {}, external client rpc : {} "
              + "shuffle: {}, webui: {}, mgmt: {}, znodePath: {}", rpcEndpoint, externalRpcEndpoint,
          getShuffleEndpoint(), getServicesEndpoint(), getMngEndpoint(), getRegistrationZnodePath());
    } else {
      LOG.info("Registered node. Created a znode on ZooKeeper for LLAP instance: rpc: {}, "
              + "shuffle: {}, webui: {}, mgmt: {}, znodePath: {}", rpcEndpoint, getShuffleEndpoint(),
          getServicesEndpoint(), getMngEndpoint(), getRegistrationZnodePath());
    }

    return uniqueId;
  }

  private void populateConfigValues(Iterable<Map.Entry<String, String>> attributes) {
    for (Map.Entry<String, String> kv : attributes) {
      if (kv.getKey().startsWith(HiveConf.PREFIX_LLAP)
          || kv.getKey().startsWith(HiveConf.PREFIX_HIVE_LLAP)) {
        // TODO: read this somewhere useful, like the task scheduler
        daemonZkRecord.set(kv.getKey(), kv.getValue());
      }
    }
  }

  @Override
  public void updateRegistration(Iterable<Map.Entry<String, String>> attributes) throws IOException {
    populateConfigValues(attributes);
    updateServiceRecord(this.daemonZkRecord, doCheckAcls, true);
  }

  @Override
  public void unregister() throws IOException {
    // Nothing for the zkCreate models
  }

  /**
   * A dynamically changing instance in an Llap Service. Can become inactive if failing or can be
   * blacklisted (set to 0 capacity) if too slow (See: BlacklistingLlapMetricsListener).
   */
  @VisibleForTesting
  public class DynamicServiceInstance
      extends ServiceInstanceBase implements LlapServiceInstance {
    private final int mngPort;
    private final int shufflePort;
    private final int outputFormatPort;
    private final String serviceAddress;

    private String externalHost;
    private int externalClientsRpcPort;

    private final Resource resource;

    public DynamicServiceInstance(ServiceRecord srv) throws IOException {
      super(srv, IPC_LLAP);

      final Endpoint shuffle = srv.getInternalEndpoint(IPC_SHUFFLE);
      final Endpoint mng = srv.getInternalEndpoint(IPC_MNG);
      final Endpoint outputFormat = srv.getInternalEndpoint(IPC_OUTPUTFORMAT);
      final Endpoint services = srv.getExternalEndpoint(IPC_SERVICES);

      this.mngPort =
          Integer.parseInt(RegistryTypeUtils.getAddressField(mng.addresses.get(0),
              AddressTypes.ADDRESS_PORT_FIELD));
      this.shufflePort =
          Integer.parseInt(RegistryTypeUtils.getAddressField(shuffle.addresses.get(0),
              AddressTypes.ADDRESS_PORT_FIELD));
      this.outputFormatPort =
          Integer.valueOf(RegistryTypeUtils.getAddressField(outputFormat.addresses.get(0),
              AddressTypes.ADDRESS_PORT_FIELD));
      this.serviceAddress =
          RegistryTypeUtils.getAddressField(services.addresses.get(0), AddressTypes.ADDRESS_URI);

      if (LlapUtil.isCloudDeployment(conf)) {
        final Endpoint externalRpc = srv.getExternalEndpoint(IPC_EXTERNAL_LLAP);
        this.externalHost = RegistryTypeUtils.getAddressField(externalRpc.addresses.get(0),
            AddressTypes.ADDRESS_HOSTNAME_FIELD);
        this.externalClientsRpcPort = Integer.parseInt(
            RegistryTypeUtils.getAddressField(externalRpc.addresses.get(0),
                AddressTypes.ADDRESS_PORT_FIELD));
      }

      String memStr = srv.get(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname, "");
      String coreStr = srv.get(LlapRegistryService.LLAP_DAEMON_NUM_ENABLED_EXECUTORS, "");
      try {
        this.resource = Resource.newInstance(Integer.parseInt(memStr), Integer.parseInt(coreStr));
      } catch (NumberFormatException ex) {
        throw new IOException("Invalid resource configuration for a LLAP node: memory "
            + memStr + ", vcores " + coreStr);
      }
    }

    @Override
    public int getShufflePort() {
      return shufflePort;
    }

    @Override
    public String getServicesAddress() {
      return serviceAddress;
    }

    @Override
    public String getExternalHostname() {
      ensureCloudEnv(LlapZookeeperRegistryImpl.this.conf);
      return externalHost;
    }

    @Override
    public int getExternalClientsRpcPort() {
      ensureCloudEnv(LlapZookeeperRegistryImpl.this.conf);
      return externalClientsRpcPort;
    }

    @Override
    public Resource getResource() {
      return resource;
    }

    @Override
    public String toString() {
      return "DynamicServiceInstance [id=" + getWorkerIdentity() + ", host=" + getHost() + ":" + getRpcPort() +
          " with resources=" + getResource() + ", shufflePort=" + getShufflePort() +
          ", servicesAddress=" + getServicesAddress() +  ", mgmtPort=" + getManagementPort() + "]";
    }

    @Override
    public int getManagementPort() {
      return mngPort;
    }

    @Override
    public int getOutputFormatPort() {
      return outputFormatPort;
    }
  }


  // TODO: this class is completely unnecessary... 1-on-1 mapping with parent.
  //       Remains here as the legacy of the original higher-level interface (getInstance).
  private static class DynamicServiceInstanceSet implements LlapServiceInstanceSet {
    private final PathChildrenCache instancesCache;
    private final LlapZookeeperRegistryImpl parent;
    private final ServiceRecordMarshal encoder;

    public DynamicServiceInstanceSet(PathChildrenCache cache,
        LlapZookeeperRegistryImpl parent, ServiceRecordMarshal encoder) {
      this.instancesCache = cache;
      this.parent = parent;
      this.encoder = encoder;
      parent.populateCache(instancesCache, false);
    }


    @Override
    public Collection<LlapServiceInstance> getAll() {
      return parent.getAllInternal();
    }

    @Override
    public Collection<LlapServiceInstance> getAllInstancesOrdered(boolean consistentIndexes) {
      return parent.getAllInstancesOrdered(consistentIndexes, instancesCache);
    }

    @Override
    public LlapServiceInstance getInstance(String name) {
      Collection<LlapServiceInstance> instances = getAll();
      for(LlapServiceInstance instance : instances) {
        if (instance.getWorkerIdentity().equals(name)) {
          return instance;
        }
      }
      return null;
    }

    @Override
    public Set<LlapServiceInstance> getByHost(String host) {
      return parent.getByHostInternal(host);
    }

    @Override
    public int size() {
      return parent.sizeInternal();
    }

    @Override
    public ApplicationId getApplicationId() {
      for (ChildData childData : instancesCache.getCurrentData()) {
        byte[] data = getWorkerData(childData, WORKER_PREFIX);
        if (data == null) continue;
        ServiceRecord sr = null;
        try {
          sr = encoder.fromBytes(childData.getPath(), data);
        } catch (IOException e) {
          LOG.error("Unable to decode data for zkpath: {}." +
              " Ignoring from current instances list..", childData.getPath());
          continue;
        }
        String containerStr = sr.get(HiveConf.ConfVars.LLAP_DAEMON_CONTAINER_ID.varname);
        if (containerStr == null || containerStr.isEmpty()) continue;
        return ContainerId.fromString(containerStr).getApplicationAttemptId().getApplicationId();
      }
      return null;
    }
  }

  private static String extractWorkerIdFromSlot(ChildData childData) {
    return new String(childData.getData(), SlotZnode.CHARSET);
  }

  // The real implementation for the instanceset... instanceset has its own copy of the
  // ZK cache yet completely depends on the parent in every other aspect and is thus unneeded.

  Collection<LlapServiceInstance> getAllInstancesOrdered(
      boolean consistentIndexes, PathChildrenCache instancesCache) {
    Map<String, Long> slotByWorker = new HashMap<String, Long>();
    Set<LlapServiceInstance> unsorted = Sets.newHashSet();
    for (ChildData childData : instancesCache.getCurrentData()) {
      if (childData == null) continue;
      byte[] data = childData.getData();
      if (data == null) continue;
      String nodeName = extractNodeName(childData);
      if (nodeName.startsWith(WORKER_PREFIX)) {
        LlapServiceInstance instances = getInstanceByPath(childData.getPath());
        if (instances != null) {
          unsorted.add(instances);
        }
      } else if (nodeName.startsWith(SLOT_PREFIX)) {
        slotByWorker.put(extractWorkerIdFromSlot(childData),
            Long.parseLong(nodeName.substring(SLOT_PREFIX.length())));
      } else {
        LOG.info("Ignoring unknown node {}", childData.getPath());
      }
    }

    TreeMap<Long, LlapServiceInstance> sorted = new TreeMap<>();
    long maxSlot = Long.MIN_VALUE;
    for (LlapServiceInstance worker : unsorted) {
      Long slot = slotByWorker.get(worker.getWorkerIdentity());
      if (slot == null) {
        LOG.info("Unknown slot for {}", worker.getWorkerIdentity());
        continue;
      }
      maxSlot = Math.max(maxSlot, slot);
      sorted.put(slot, worker);
    }

    if (consistentIndexes) {
      // Add dummy instances to all slots where LLAPs are MIA... I can haz insert_iterator? 
      TreeMap<Long, LlapServiceInstance> dummies = new TreeMap<>();
      Iterator<Long> keyIter = sorted.keySet().iterator();
      long expected = 0;
      Long ts = null;
      while (keyIter.hasNext()) {
        Long slot = keyIter.next();
        assert slot >= expected;
        while (slot > expected) {
          if (ts == null) {
            ts = System.nanoTime(); // Inactive nodes restart every call!
          }
          dummies.put(expected, new InactiveServiceInstance("inactive-" + expected + "-" + ts));
          ++expected;
        }
        ++expected;
      }
      sorted.putAll(dummies);
    }
    return sorted.values();
  }

  private static String extractNodeName(ChildData childData) {
    String nodeName = childData.getPath();
    int ix = nodeName.lastIndexOf("/");
    if (ix >= 0) {
      nodeName = nodeName.substring(ix + 1);
    }
    return nodeName;
  }


  @Override
  public LlapServiceInstanceSet getInstances(
      String component, long clusterReadyTimeoutMs) throws IOException {
    PathChildrenCache instancesCache = ensureInstancesCache(clusterReadyTimeoutMs);

    // lazily create instances
    if (instances == null) {
      this.instances = new DynamicServiceInstanceSet(instancesCache, this, encoder);
    }
    return instances;
  }

  @Override
  public ApplicationId getApplicationId() throws IOException {
    return getInstances("LLAP", 0).getApplicationId();
  }

  @Override
  public void stop() {
    CloseableUtils.closeQuietly(slotZnode);
    super.stop();
  }

  @Override
  protected LlapServiceInstance createServiceInstance(ServiceRecord srv) throws IOException {
    return new DynamicServiceInstance(srv);
  }

  @Override
  protected String getZkPathUser(Configuration conf) {
    // External LLAP clients would need to set LLAP_ZK_REGISTRY_USER to the LLAP daemon user (hive),
    // rather than relying on LlapRegistryService.currentUser().
    return HiveConf.getVar(conf, ConfVars.LLAP_ZK_REGISTRY_USER, LlapRegistryService.currentUser());
  }

  /**
   * Locks the Llap Cluster for configuration change for the given time window.
   * @param windowStart The beginning of the time window when no other configuration change is allowed.
   * @param windowEnd The end of the time window when no other configuration change is allowed.
   * @return The result of the change (success if the lock is succeeded, and the next possible
   * configuration change time
   */
  public ConfigChangeLockResult lockForConfigChange(long windowStart, long windowEnd) {
    if (windowEnd < windowStart) {
      throw new IllegalArgumentException(
          "WindowStart=" + windowStart + " can not be smaller than WindowEnd=" + windowEnd);
    }
    try {
      if (lockWindowEnd == null) {
        // Create the node with the /llap-sasl/hiveuser/hostname/config-change/next-change path without retry
        lockWindowEnd = new DistributedAtomicLong(zooKeeperClient,
            String.join("/", workersPath.substring(0, workersPath.lastIndexOf('/')), CONFIG_CHANGE_PATH,
                CONFIG_CHANGE_NODE), (i, j, sleeper) -> false);
        lockWindowEnd.initialize(0L);
      }
      AtomicValue<Long> current = lockWindowEnd.get();
      if (!current.succeeded()) {
        LOG.debug("Can not get the current configuration lock time");
        return new ConfigChangeLockResult(false, -1L);
      }
      if (current.postValue() > windowStart) {
        LOG.debug("Can not lock window {}-{}. Current value is {}.", windowStart, windowEnd, current.postValue());
        return new ConfigChangeLockResult(false, current.postValue());
      }
      current = lockWindowEnd.compareAndSet(current.postValue(), windowEnd);
      if (!current.succeeded()) {
        LOG.debug("Can not lock window {}-{}. Current value is changed to {}.", windowStart, windowEnd,
            current.postValue());
        return new ConfigChangeLockResult(false, current.postValue());
      }
      return new ConfigChangeLockResult(true, current.postValue());
    } catch (Throwable t) {
      LOG.info("Can not reserve configuration change lock", t);
      return new ConfigChangeLockResult(false, -1L);
    }
  }

  /**
   * The return data of a config change. Successful or not successful and the next time a config
   * change can be attempted.
   */
  public static class ConfigChangeLockResult {
    private final boolean success;
    private final long nextConfigChangeTime;

    @VisibleForTesting
    public ConfigChangeLockResult(boolean success, long nextConfigChangeTime) {
      this.success = success;
      this.nextConfigChangeTime = nextConfigChangeTime;
    }

    public boolean isSuccess() {
      return success;
    }

    public long getNextConfigChangeTime() {
      return nextConfigChangeTime;
    }

    @Override
    public String toString() {
      return "ConfigChangeLockResult [" + success + "," + nextConfigChangeTime + "]";
    }
  }
}
