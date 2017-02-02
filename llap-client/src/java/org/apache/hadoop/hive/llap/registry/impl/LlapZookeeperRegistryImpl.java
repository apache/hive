/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.llap.registry.impl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.AppConfigurationEntry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.registry.ServiceInstance;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.ServiceInstanceStateChangeListener;
import org.apache.hadoop.hive.llap.registry.ServiceRegistry;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils.ServiceRecordMarshal;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LlapZookeeperRegistryImpl implements ServiceRegistry {

  private static final Logger LOG = LoggerFactory.getLogger(LlapZookeeperRegistryImpl.class);

  /**
   * IPC endpoint names.
   */
  private static final String IPC_SERVICES = "services";
  private static final String IPC_MNG = "llapmng";
  private static final String IPC_SHUFFLE = "shuffle";
  private static final String IPC_LLAP = "llap";
  private static final String IPC_OUTPUTFORMAT = "llapoutputformat";
  private final static String SASL_NAMESPACE = "llap-sasl";
  private final static String UNSECURE_NAMESPACE = "llap-unsecure";
  private final static String USER_SCOPE_PATH_PREFIX = "user-";
  private static final String DISABLE_MESSAGE =
      "Set " + ConfVars.LLAP_VALIDATE_ACLS.varname + " to false to disable ACL validation";
  private static final String WORKER_PREFIX = "worker-";
  private static final String SLOT_PREFIX = "slot-";

  private final Configuration conf;
  private final CuratorFramework zooKeeperClient;
  // userPathPrefix is the path specific to the user for which ACLs should be restrictive.
  // workersPath is the directory path where all the worker znodes are located.
  private final String userPathPrefix, workersPath;
  private String userNameFromPrincipal; // Only set when setting up the secure config for ZK.

  private PersistentEphemeralNode znode;

  private SlotZnode slotZnode;
  private String znodePath; // unique identity for this instance
  private final ServiceRecordMarshal encoder; // to marshal/unmarshal znode data

  // to be used by clients of ServiceRegistry
  private DynamicServiceInstanceSet instances;
  private PathChildrenCache instancesCache;

  private static final UUID uniq = UUID.randomUUID();
  private static final String UNIQUE_IDENTIFIER = "llap.unique.id";

  private Set<ServiceInstanceStateChangeListener> stateChangeListeners;

  // get local hostname
  private static final String hostname;

  static {
    String localhost = "localhost";
    try {
      localhost = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException uhe) {
      // ignore
    }
    hostname = localhost;
  }

  public LlapZookeeperRegistryImpl(String instanceName, Configuration conf) {
    this.conf = new Configuration(conf);
    this.conf.addResource(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    String zkEnsemble = getQuorumServers(this.conf);
    this.encoder = new RegistryUtils.ServiceRecordMarshal();
    int sessionTimeout = (int) HiveConf.getTimeVar(conf, ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT,
        TimeUnit.MILLISECONDS);
    int baseSleepTime = (int) HiveConf
        .getTimeVar(conf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME,
            TimeUnit.MILLISECONDS);
    int maxRetries = HiveConf.getIntVar(conf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);

    // sample path: /llap-sasl/hiveuser/hostname/workers/worker-0000000
    // worker-0000000 is the sequence number which will be retained until session timeout. If a
    // worker does not respond due to communication interruptions it will retain the same sequence
    // number when it returns back. If session timeout expires, the node will be deleted and new
    // addition of the same node (restart) will get next sequence number
    this.userPathPrefix = USER_SCOPE_PATH_PREFIX + getZkPathUser(this.conf);
    this.workersPath =  "/" + userPathPrefix + "/" + instanceName + "/workers";
    this.instancesCache = null;
    this.instances = null;
    this.stateChangeListeners = new HashSet<>();

    final boolean isSecure = UserGroupInformation.isSecurityEnabled();
    ACLProvider zooKeeperAclProvider = new ACLProvider() {
      @Override
      public List<ACL> getDefaultAcl() {
        // We always return something from getAclForPath so this should not happen.
        LOG.warn("getDefaultAcl was called");
        return Lists.newArrayList(ZooDefs.Ids.OPEN_ACL_UNSAFE);
      }

      @Override
      public List<ACL> getAclForPath(String path) {
        if (!isSecure || path == null || !path.contains(userPathPrefix)) {
          // No security or the path is below the user path - full access.
          return Lists.newArrayList(ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
        return createSecureAcls();
      }
    };
    String rootNs = HiveConf.getVar(conf, ConfVars.LLAP_ZK_REGISTRY_NAMESPACE);
    if (rootNs == null) {
      rootNs = isSecure ? SASL_NAMESPACE : UNSECURE_NAMESPACE; // The normal path.
    }

    // Create a CuratorFramework instance to be used as the ZooKeeper client
    // Use the zooKeeperAclProvider to create appropriate ACLs
    this.zooKeeperClient = CuratorFrameworkFactory.builder()
        .connectString(zkEnsemble)
        .sessionTimeoutMs(sessionTimeout)
        .aclProvider(zooKeeperAclProvider)
        .namespace(rootNs)
        .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
        .build();

    LOG.info("Llap Zookeeper Registry is enabled with registryid: " + instanceName);
  }

  private static List<ACL> createSecureAcls() {
    // Read all to the world
    List<ACL> nodeAcls = new ArrayList<ACL>(ZooDefs.Ids.READ_ACL_UNSAFE);
    // Create/Delete/Write/Admin to creator
    nodeAcls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
    return nodeAcls;
  }

  /**
   * Get the ensemble server addresses from the configuration. The format is: host1:port,
   * host2:port..
   *
   * @param conf
   **/
  private String getQuorumServers(Configuration conf) {
    String[] hosts = conf.getTrimmedStrings(ConfVars.HIVE_ZOOKEEPER_QUORUM.varname);
    String port = conf.get(ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.varname,
        ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.getDefaultValue());
    StringBuilder quorum = new StringBuilder();
    for (int i = 0; i < hosts.length; i++) {
      quorum.append(hosts[i].trim());
      if (!hosts[i].contains(":")) {
        // if the hostname doesn't contain a port, add the configured port to hostname
        quorum.append(":");
        quorum.append(port);
      }

      if (i != hosts.length - 1) {
        quorum.append(",");
      }
    }

    return quorum.toString();
  }

  private String getZkPathUser(Configuration conf) {
    // External LLAP clients would need to set LLAP_ZK_REGISTRY_USER to the LLAP daemon user (hive),
    // rather than relying on RegistryUtils.currentUser().
    String user = HiveConf.getVar(conf, ConfVars.LLAP_ZK_REGISTRY_USER, RegistryUtils.currentUser());
    return user;
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

  @Override
  public String register() throws IOException {
    ServiceRecord srv = new ServiceRecord();
    Endpoint rpcEndpoint = getRpcEndpoint();
    srv.addInternalEndpoint(rpcEndpoint);
    srv.addInternalEndpoint(getMngEndpoint());
    srv.addInternalEndpoint(getShuffleEndpoint());
    srv.addExternalEndpoint(getServicesEndpoint());
    srv.addInternalEndpoint(getOutputFormatEndpoint());

    for (Map.Entry<String, String> kv : this.conf) {
      if (kv.getKey().startsWith(HiveConf.PREFIX_LLAP)
          || kv.getKey().startsWith(HiveConf.PREFIX_HIVE_LLAP)) {
        // TODO: read this somewhere useful, like the task scheduler
        srv.set(kv.getKey(), kv.getValue());
      }
    }

    // restart sensitive instance id
    srv.set(UNIQUE_IDENTIFIER, uniq.toString());

    // Create a znode under the rootNamespace parent for this instance of the server
    try {
      // PersistentEphemeralNode will make sure the ephemeral node created on server will be present
      // even under connection or session interruption (will automatically handle retries)
      znode = new PersistentEphemeralNode(zooKeeperClient, Mode.EPHEMERAL_SEQUENTIAL,
          workersPath + "/" + WORKER_PREFIX, encoder.toBytes(srv));

      // start the creation of znodes
      znode.start();

      // We'll wait for 120s for node creation
      long znodeCreationTimeout = 120;
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception(
            "Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
      }

      znodePath = znode.getActualPath();

      slotZnode = new SlotZnode(
          zooKeeperClient, workersPath, SLOT_PREFIX, WORKER_PREFIX, uniq.toString());
      if (!slotZnode.start(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception(
            "Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
      }

      if (HiveConf.getBoolVar(conf, ConfVars.LLAP_VALIDATE_ACLS)) {
        try {
          checkAndSetAcls();
        } catch (Exception ex) {
          throw new IOException("Error validating or setting ACLs. " + DISABLE_MESSAGE, ex);
        }
      }
      if (zooKeeperClient.checkExists().forPath(znodePath) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode for this LLAP instance on ZooKeeper.");
      }
      LOG.info(
          "Registered node. Created a znode on ZooKeeper for LLAP instance: rpc: {}, shuffle: {}," +
              " webui: {}, mgmt: {}, znodePath: {} ",
          rpcEndpoint, getShuffleEndpoint(), getServicesEndpoint(), getMngEndpoint(), znodePath);
    } catch (Exception e) {
      LOG.error("Unable to create a znode for this server instance", e);
      CloseableUtils.closeQuietly(znode);
      CloseableUtils.closeQuietly(slotZnode);
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Created zknode with path: {} service record: {}", znodePath, srv);
    }

    return uniq.toString();
  }

  private void checkAndSetAcls() throws Exception {
    if (!UserGroupInformation.isSecurityEnabled()) return;
    // We are trying to check ACLs on the "workers" directory, which noone except us should be
    // able to write to. Higher-level directories shouldn't matter - we don't read them.
    String pathToCheck = workersPath;
    List<ACL> acls = zooKeeperClient.getACL().forPath(pathToCheck);
    if (acls == null || acls.isEmpty()) {
      // Can there be no ACLs? There's some access (to get ACLs), so assume it means free for all.
      LOG.warn("No ACLs on "  + pathToCheck + "; setting up ACLs. " + DISABLE_MESSAGE);
      setUpAcls(pathToCheck);
      return;
    }
    // This could be brittle.
    assert userNameFromPrincipal != null;
    Id currentUser = new Id("sasl", userNameFromPrincipal);
    for (ACL acl : acls) {
      if ((acl.getPerms() & ~ZooDefs.Perms.READ) == 0 || currentUser.equals(acl.getId())) {
        continue; // Read permission/no permissions, or the expected user.
      }
      LOG.warn("The ACL " + acl + " is unnacceptable for " + pathToCheck
        + "; setting up ACLs. " + DISABLE_MESSAGE);
      setUpAcls(pathToCheck);
      return;
    }
  }

  private void setUpAcls(String path) throws Exception {
    List<ACL> acls = createSecureAcls();
    LinkedList<String> paths = new LinkedList<>();
    paths.add(path);
    while (!paths.isEmpty()) {
      String currentPath = paths.poll();
      List<String> children = zooKeeperClient.getChildren().forPath(currentPath);
      if (children != null) {
        for (String child : children) {
          paths.add(currentPath + "/" + child);
        }
      }
      zooKeeperClient.setACL().withACL(acls).forPath(currentPath);
    }
  }


  @Override
  public void unregister() throws IOException {
    // Nothing for the zkCreate models
  }

  private class DynamicServiceInstance implements ServiceInstance {

    private final ServiceRecord srv;
    private final String host;
    private final int rpcPort;
    private final int mngPort;
    private final int shufflePort;
    private final int outputFormatPort;
    private final String serviceAddress;

    public DynamicServiceInstance(ServiceRecord srv) throws IOException {
      this.srv = srv;

      if (LOG.isTraceEnabled()) {
        LOG.trace("Working with ServiceRecord: {}", srv);
      }

      final Endpoint shuffle = srv.getInternalEndpoint(IPC_SHUFFLE);
      final Endpoint rpc = srv.getInternalEndpoint(IPC_LLAP);
      final Endpoint mng = srv.getInternalEndpoint(IPC_MNG);
      final Endpoint outputFormat = srv.getInternalEndpoint(IPC_OUTPUTFORMAT);
      final Endpoint services = srv.getExternalEndpoint(IPC_SERVICES);

      this.host =
          RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
              AddressTypes.ADDRESS_HOSTNAME_FIELD);
      this.rpcPort =
          Integer.parseInt(RegistryTypeUtils.getAddressField(rpc.addresses.get(0),
              AddressTypes.ADDRESS_PORT_FIELD));
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
    }

    @Override
    public String getWorkerIdentity() {
      return srv.get(UNIQUE_IDENTIFIER);
    }

    @Override
    public String getHost() {
      return host;
    }

    @Override
    public int getRpcPort() {
      return rpcPort;
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
    public Map<String, String> getProperties() {
      return srv.attributes();
    }

    @Override
    public Resource getResource() {
      int memory = Integer.parseInt(srv.get(ConfVars.LLAP_DAEMON_MEMORY_PER_INSTANCE_MB.varname));
      int vCores = Integer.parseInt(srv.get(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname));
      return Resource.newInstance(memory, vCores);
    }

    @Override
    public String toString() {
      return "DynamicServiceInstance [id=" + getWorkerIdentity() + ", host=" + host + ":" + rpcPort +
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

    // TODO: This needs a hashCode/equality implementation if used as a key in various structures.
    // A new ServiceInstance is created each time.
  }

  private class DynamicServiceInstanceSet implements ServiceInstanceSet {
    private final PathChildrenCache instancesCache;

    public DynamicServiceInstanceSet(final PathChildrenCache cache) {
      this.instancesCache = cache;
    }

    @Override
    public Collection<ServiceInstance> getAll() {
      List<ServiceInstance> instances = new ArrayList<>();
      // TODO: we could refresh instanceCache here on previous failure
      for (ChildData childData : instancesCache.getCurrentData()) {
        if (childData == null) continue;
        byte[] data = childData.getData();
        if (data == null) continue;
        if (!extractNodeName(childData).startsWith(WORKER_PREFIX)) continue;
        try {
          ServiceRecord srv = encoder.fromBytes(childData.getPath(), data);
          ServiceInstance instance = new DynamicServiceInstance(srv);
          instances.add(instance);
        } catch (IOException e) {
          LOG.error("Unable to decode data for zkpath: {}." +
              " Ignoring from current instances list..", childData.getPath());
        }
      }
      return instances;
    }

    @Override
    public Collection<ServiceInstance> getAllInstancesOrdered(boolean consistentIndexes) {
      Map<String, Long> slotByWorker = new HashMap<String, Long>();
      List<ServiceInstance> unsorted = new LinkedList<ServiceInstance>();
      for (ChildData childData : instancesCache.getCurrentData()) {
        if (childData == null) continue;
        byte[] data = childData.getData();
        if (data == null) continue;
        String nodeName = extractNodeName(childData);
        if (nodeName.startsWith(WORKER_PREFIX)) {
          try {
            ServiceRecord srv = encoder.fromBytes(childData.getPath(), data);
            ServiceInstance instance = new DynamicServiceInstance(srv);
            unsorted.add(instance);
          } catch (IOException e) {
            LOG.error("Unable to decode data for zkpath: {}." +
                " Ignoring from current instances list..", childData.getPath());
          }
        } else if (nodeName.startsWith(SLOT_PREFIX)) {
          slotByWorker.put(extractWorkerIdFromSlot(childData),
              Long.parseLong(nodeName.substring(SLOT_PREFIX.length())));
        } else {
          LOG.info("Ignoring unknown node {}", childData.getPath());
        }
      }

      TreeMap<Long, ServiceInstance> sorted = new TreeMap<>();
      long maxSlot = Long.MIN_VALUE;
      for (ServiceInstance worker : unsorted) {
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
        TreeMap<Long, ServiceInstance> dummies = new TreeMap<>();
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

    @Override
    public ServiceInstance getInstance(String name) {
      Collection<ServiceInstance> instances = getAll();
      for(ServiceInstance instance : instances) {
        if (instance.getWorkerIdentity().equals(name)) {
          return instance;
        }
      }
      return null;
    }

    @Override
    public Set<ServiceInstance> getByHost(String host) {
      Set<ServiceInstance> byHost = new HashSet<>();
      for (ChildData childData : instancesCache.getCurrentData()) {
        if (childData == null) continue;
        byte[] data = childData.getData();
        if (data == null) continue;
        if (!extractNodeName(childData).startsWith(WORKER_PREFIX)) continue;
        try {
          ServiceRecord srv = encoder.fromBytes(childData.getPath(), data);
          ServiceInstance instance = new DynamicServiceInstance(srv);
          if (host.equals(instance.getHost())) {
            byHost.add(instance);
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug("Locality comparing " + host + " to " + instance.getHost());
          }
        } catch (IOException e) {
          LOG.error("Unable to decode data for zkpath: {}." +
              " Ignoring host from current instances list..", childData.getPath());
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Returning " + byHost.size() + " hosts for locality allocation on " + host);
      }
      return byHost;
    }

    @Override
    public int size() {
      return instancesCache.getCurrentData().size();
    }
  }

  // TODO: make class static? fields leak
  private class InstanceStateChangeListener implements PathChildrenCacheListener {
    private final Logger LOG = LoggerFactory.getLogger(InstanceStateChangeListener.class);

    @Override
    public void childEvent(final CuratorFramework client, final PathChildrenCacheEvent event)
        throws Exception {
      Preconditions.checkArgument(client != null
          && client.getState() == CuratorFrameworkState.STARTED, "client is not started");

      synchronized (this) {
        if (stateChangeListeners.isEmpty()) return;
        ChildData childData = event.getData();
        if (childData == null) return;
        String nodeName = extractNodeName(childData);
        if (!nodeName.startsWith(WORKER_PREFIX)) return; // No need to propagate slot updates.
        LOG.info("{} for zknode {} in llap namespace", event.getType(), childData.getPath());
        ServiceInstance instance = extractServiceInstance(event, childData);
        for (ServiceInstanceStateChangeListener listener : stateChangeListeners) {
          switch (event.getType()) {
          case CHILD_ADDED:
            listener.onCreate(instance);
            break;
          case CHILD_UPDATED:
            listener.onUpdate(instance);
            break;
          case CHILD_REMOVED:
            listener.onRemove(instance);
            break;
          default:
            // Ignore all the other events; logged above.
          }
        }
      }
    }
  }

  private static String extractWorkerIdFromSlot(ChildData childData) {
    return new String(childData.getData(), SlotZnode.CHARSET);
  }

  private static String extractNodeName(ChildData childData) {
    String nodeName = childData.getPath();
    int ix = nodeName.lastIndexOf("/");
    if (ix >= 0) {
      nodeName = nodeName.substring(ix + 1);
    }
    return nodeName;
  }

  private ServiceInstance extractServiceInstance(
      PathChildrenCacheEvent event, ChildData childData) {
    byte[] data = childData.getData();
    if (data == null) return null;
    try {
      ServiceRecord srv = encoder.fromBytes(event.getData().getPath(), data);
      return new DynamicServiceInstance(srv);
    } catch (IOException e) {
      LOG.error("Unable to decode data for zknode: {}." +
          " Dropping notification of type: {}", childData.getPath(), event.getType());
      return null;
    }
  }
  
  @Override
  public ServiceInstanceSet getInstances(String component) throws IOException {
    checkPathChildrenCache();

    // lazily create instances
    if (instances == null) {
      this.instances = new DynamicServiceInstanceSet(instancesCache);
    }
    return instances;
  }

  @Override
  public synchronized void registerStateChangeListener(
      final ServiceInstanceStateChangeListener listener)
      throws IOException {
    checkPathChildrenCache();

    this.stateChangeListeners.add(listener);
  }

  private synchronized void checkPathChildrenCache() throws IOException {
    Preconditions.checkArgument(zooKeeperClient != null &&
            zooKeeperClient.getState() == CuratorFrameworkState.STARTED,
        "client is not started");

    // lazily create PathChildrenCache
    if (instancesCache == null) {
      this.instancesCache = new PathChildrenCache(zooKeeperClient, workersPath, true);
      instancesCache.getListenable().addListener(new InstanceStateChangeListener(),
          Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("StateChangeNotificationHandler")
              .build()));
      try {
        this.instancesCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
      } catch (Exception e) {
        LOG.error("Unable to start curator PathChildrenCache. Exception: {}", e);
        throw new IOException(e);
      }
    }
  }

  @Override
  public void start() throws IOException {
    if (zooKeeperClient != null) {
      setupZookeeperAuth(this.conf);
      zooKeeperClient.start();
    }
    // Init closeable utils in case register is not called (see HIVE-13322)
    CloseableUtils.class.getName();
  }

  @Override
  public void stop() throws IOException {
    CloseableUtils.closeQuietly(znode);
    CloseableUtils.closeQuietly(slotZnode);
    CloseableUtils.closeQuietly(instancesCache);
    CloseableUtils.closeQuietly(zooKeeperClient);
  }


  private void setupZookeeperAuth(final Configuration conf) throws IOException {
    if (UserGroupInformation.isSecurityEnabled() && LlapProxy.isDaemon()) {
      LOG.info("UGI security is enabled. Setting up ZK auth.");

      String llapPrincipal = HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_PRINCIPAL);
      if (llapPrincipal == null || llapPrincipal.isEmpty()) {
        throw new IOException("Llap Kerberos principal is empty");
      }

      String llapKeytab = HiveConf.getVar(conf, ConfVars.LLAP_KERBEROS_KEYTAB_FILE);
      if (llapKeytab == null || llapKeytab.isEmpty()) {
        throw new IOException("Llap Kerberos keytab is empty");
      }

      // Install the JAAS Configuration for the runtime
      setZookeeperClientKerberosJaasConfig(llapPrincipal, llapKeytab);
    } else {
      LOG.info("UGI security is not enabled, or non-daemon environment. Skipping setting up ZK auth.");
    }
  }

  /**
   * Dynamically sets up the JAAS configuration that uses kerberos
   *
   * @param principal
   * @param keyTabFile
   * @throws IOException
   */
  private void setZookeeperClientKerberosJaasConfig(String principal, String keyTabFile)
      throws IOException {
    // ZooKeeper property name to pick the correct JAAS conf section
    final String SASL_LOGIN_CONTEXT_NAME = "LlapZooKeeperClient";
    System.setProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY, SASL_LOGIN_CONTEXT_NAME);

    principal = SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
    userNameFromPrincipal = LlapUtil.getUserNameFromPrincipal(principal);
    JaasConfiguration jaasConf = new JaasConfiguration(SASL_LOGIN_CONTEXT_NAME, principal,
        keyTabFile);

    // Install the Configuration in the runtime.
    javax.security.auth.login.Configuration.setConfiguration(jaasConf);
  }

  /**
   * A JAAS configuration for ZooKeeper clients intended to use for SASL
   * Kerberos.
   */
  private static class JaasConfiguration extends javax.security.auth.login.Configuration {
    // Current installed Configuration
    private final javax.security.auth.login.Configuration baseConfig = javax.security.auth.login.Configuration
        .getConfiguration();
    private final String loginContextName;
    private final String principal;
    private final String keyTabFile;

    public JaasConfiguration(String llapLoginContextName, String principal, String keyTabFile) {
      this.loginContextName = llapLoginContextName;
      this.principal = principal;
      this.keyTabFile = keyTabFile;
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (loginContextName.equals(appName)) {
        Map<String, String> krbOptions = new HashMap<String, String>();
        krbOptions.put("doNotPrompt", "true");
        krbOptions.put("storeKey", "true");
        krbOptions.put("useKeyTab", "true");
        krbOptions.put("principal", principal);
        krbOptions.put("keyTab", keyTabFile);
        krbOptions.put("refreshKrb5Config", "true");
        AppConfigurationEntry llapZooKeeperClientEntry = new AppConfigurationEntry(
            KerberosUtil.getKrb5LoginModuleName(),
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, krbOptions);
        return new AppConfigurationEntry[]{llapZooKeeperClientEntry};
      }
      // Try the base config
      if (baseConfig != null) {
        return baseConfig.getAppConfigurationEntry(appName);
      }
      return null;
    }
  }
}
