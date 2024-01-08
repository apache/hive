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
package org.apache.hadoop.hive.registry.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.registry.RegistryUtilities;
import org.apache.hadoop.hive.registry.ServiceInstance;
import org.apache.hadoop.hive.registry.ServiceInstanceStateChangeListener;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils.ServiceRecordMarshal;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.InvalidACLException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * This is currently used for implementation inheritance only; it doesn't provide a unified flow
 * into which one can just plug a few abstract method implementations, because providing one with
 * getInstance method is a huge pain involving lots of generics. Also, different registries may
 * have slightly different usage patterns anyway and noone would use a registry without knowing
 * what type it is. So, it's mostly a grab bag of methods used by ServiceInstanceSet and other
 * parts of each implementation.
 */
public abstract class ZkRegistryBase<InstanceType extends ServiceInstance> {
  private static final Logger LOG = LoggerFactory.getLogger(ZkRegistryBase.class);
  private final static String SASL_NAMESPACE = "sasl";
  private final static String UNSECURE_NAMESPACE = "unsecure";
  protected final static String USER_SCOPE_PATH_PREFIX = "user-";
  protected static final String WORKER_PREFIX = "worker-";
  protected static final String WORKER_GROUP = "workers";
  public static final String UNIQUE_IDENTIFIER = "registry.unique.id";
  protected static final UUID UNIQUE_ID = UUID.randomUUID();
  private static final Joiner PATH_JOINER = Joiner.on("/").skipNulls();

  protected final Configuration conf;
  protected final CuratorFramework zooKeeperClient;
  // workersPath is the directory path where all the worker znodes are located.
  protected final String workersPath;
  private final String workerNodePrefix;

  protected final ServiceRecordMarshal encoder; // to marshal/unmarshal znode data

  private final Set<ServiceInstanceStateChangeListener<InstanceType>> stateChangeListeners;

  protected final boolean doCheckAcls;
  // Secure ZK is only set up by the registering service; anyone can read the registrations.
  private final String zkPrincipal, zkKeytab, saslLoginContextName;
  private String userNameFromPrincipal; // Only set when setting up the secure config for ZK.
  private final String disableMessage;

  private final Lock instanceCacheLock = new ReentrantLock();
  // there can be only one instance per path
  private final Map<String, InstanceType> pathToInstanceCache;
  // there can be multiple instances per node
  private final Map<String, Set<InstanceType>> nodeToInstanceCache;

  // The registration znode.
  private PersistentNode znode;
  private String znodePath; // unique identity for this instance

  final String namespace;

  private PathChildrenCache instancesCache; // Created on demand.

  /** Local hostname. */
  protected static final String hostname = RegistryUtilities.getCanonicalHostName();

  /**
   * @param rootNs A single root namespace override. Not recommended.
   * @param nsPrefix The namespace prefix to use with default namespaces (appends 'sasl' for secure else 'unsecure'
   *                 to namespace prefix to get effective root namespace).
   * @param userScopePathPrefix The prefix to use for the user-specific part of the path.
   * @param workerPrefix The prefix to use for each worker znode.
   * @param workerGroup group name to use for all workers
   * @param zkSaslLoginContextName SASL login context name for ZK security; null if not needed.
   * @param zkPrincipal ZK security principal.
   * @param zkKeytab ZK security keytab.
   * @param aclsConfig A config setting to use to determine if ACLs should be verified.
   */
  public ZkRegistryBase(String instanceName, Configuration conf, String rootNs, String nsPrefix,
      String userScopePathPrefix, String workerPrefix, String workerGroup,
      String zkSaslLoginContextName, String zkPrincipal, String zkKeytab, ConfVars aclsConfig) {
    this.conf = new Configuration(conf);
    this.saslLoginContextName = zkSaslLoginContextName;
    this.zkPrincipal = zkPrincipal;
    this.zkKeytab = zkKeytab;
    if (aclsConfig != null) {
      this.doCheckAcls = HiveConf.getBoolVar(conf, aclsConfig);
      this.disableMessage = "Set " + aclsConfig.varname + " to false to disable ACL validation";
    } else {
      this.doCheckAcls = true;
      this.disableMessage = "";
    }
    this.conf.addResource(YarnConfiguration.YARN_SITE_CONFIGURATION_FILE);
    this.encoder = new RegistryUtils.ServiceRecordMarshal();

    // sample path: /llap-sasl/hiveuser/hostname/workers/worker-0000000
    // worker-0000000 is the sequence number which will be retained until session timeout. If a
    // worker does not respond due to communication interruptions it will retain the same sequence
    // number when it returns back. If session timeout expires, the node will be deleted and new
    // addition of the same node (restart) will get next sequence number
    final String userPathPrefix = userScopePathPrefix == null ? null : userScopePathPrefix + getZkPathUser(conf);
    this.workerNodePrefix = workerPrefix == null ? WORKER_PREFIX : workerPrefix;
    this.workersPath =  "/" + PATH_JOINER.join(userPathPrefix, instanceName, workerGroup);
    this.instancesCache = null;
    this.stateChangeListeners = new HashSet<>();
    this.pathToInstanceCache = new ConcurrentHashMap<>();
    this.nodeToInstanceCache = new ConcurrentHashMap<>();
    this.namespace = getRootNamespace(conf, rootNs, nsPrefix);
    ACLProvider aclProvider;
    // get acl provider for most outer path that is non-null
    if (userPathPrefix == null) {
      if (instanceName == null) {
        if (workerGroup == null) {
          aclProvider = getACLProviderForZKPath(namespace);
        } else {
          aclProvider = getACLProviderForZKPath(workerGroup);
        }
      } else {
        aclProvider = getACLProviderForZKPath(instanceName);
      }
    } else {
      aclProvider = getACLProviderForZKPath(userScopePathPrefix);
    }
    this.zooKeeperClient = getZookeeperClient(conf, namespace, aclProvider);
    this.zooKeeperClient.getConnectionStateListenable().addListener(new ZkConnectionStateListener());
  }

  public static String getRootNamespace(Configuration conf, String userProvidedNamespace,
      String defaultNamespacePrefix) {
    final boolean isSecure = ZookeeperUtils.isKerberosEnabled(conf);
    String rootNs = userProvidedNamespace;
    if (rootNs == null) {
      rootNs = defaultNamespacePrefix + (isSecure ? SASL_NAMESPACE : UNSECURE_NAMESPACE);
    }
    return rootNs;
  }

  private ACLProvider getACLProviderForZKPath(String zkPath) {
    final boolean isSecure = ZookeeperUtils.isKerberosEnabled(conf);
    return new ACLProvider() {
      @Override
      public List<ACL> getDefaultAcl() {
        // We always return something from getAclForPath so this should not happen.
        LOG.warn("getDefaultAcl was called");
        return Lists.newArrayList(ZooDefs.Ids.OPEN_ACL_UNSAFE);
      }

      @Override
      public List<ACL> getAclForPath(String path) {
        if (!isSecure || path == null || !path.contains(zkPath)) {
          // No security or the path is below the user path - full access.
          return Lists.newArrayList(ZooDefs.Ids.OPEN_ACL_UNSAFE);
        }
        return createSecureAcls();
      }
    };
  }

  private CuratorFramework getZookeeperClient(Configuration conf, String namespace, ACLProvider zooKeeperAclProvider) {
    String keyStorePassword = "";
    String trustStorePassword = "";
    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_ENABLE)) {
      try {
        keyStorePassword =
            ShimLoader.getHadoopShims().getPassword(conf, ConfVars.HIVE_ZOOKEEPER_SSL_KEYSTORE_PASSWORD.varname);
        trustStorePassword =
            ShimLoader.getHadoopShims().getPassword(conf, ConfVars.HIVE_ZOOKEEPER_SSL_TRUSTSTORE_PASSWORD.varname);
      } catch (Exception e) {
        throw new RuntimeException("Failed to read zookeeper conf passwords", e);
      }
    }
    return ZooKeeperHiveHelper.builder()
        .quorum(conf.get(ConfVars.HIVE_ZOOKEEPER_QUORUM.varname))
        .clientPort(conf.get(ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.varname,
            ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT.getDefaultValue()))
        .connectionTimeout(
            (int) HiveConf.getTimeVar(conf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS))
        .sessionTimeout(
            (int) HiveConf.getTimeVar(conf, ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS))
        .baseSleepTime(
            (int) HiveConf.getTimeVar(conf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME, TimeUnit.MILLISECONDS))
        .maxRetries(HiveConf.getIntVar(conf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES))
        .sslEnabled(HiveConf.getBoolVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_ENABLE))
        .keyStoreLocation(HiveConf.getVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_KEYSTORE_LOCATION))
        .keyStorePassword(keyStorePassword)
        .keyStoreType(HiveConf.getVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_KEYSTORE_TYPE))
        .trustStoreLocation(HiveConf.getVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_TRUSTSTORE_LOCATION))
        .trustStorePassword(trustStorePassword)
        .trustStoreType(HiveConf.getVar(conf, ConfVars.HIVE_ZOOKEEPER_SSL_TRUSTSTORE_TYPE))
        .build().getNewZookeeperClient(zooKeeperAclProvider, namespace);
  }

  private static List<ACL> createSecureAcls() {
    // Read all to the world
    List<ACL> nodeAcls = new ArrayList<>(ZooDefs.Ids.READ_ACL_UNSAFE);
    // Create/Delete/Write/Admin to creator
    nodeAcls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
    return nodeAcls;
  }

  /**
   * Get the ensemble server addresses from the configuration. The format is: host1:port,
   * host2:port..
   *
   * @param conf configuration
   **/
  private static String getQuorumServers(Configuration conf) {
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

  protected abstract String getZkPathUser(Configuration conf);

  protected final String registerServiceRecord(ServiceRecord srv) throws IOException {
    return registerServiceRecord(srv, UNIQUE_ID.toString());
  }

  protected final String registerServiceRecord(ServiceRecord srv, final String uniqueId) throws IOException {
    // restart sensitive instance id
    srv.set(UNIQUE_IDENTIFIER, uniqueId);

    // Create a znode under the rootNamespace parent for this instance of the server
    try {
      // PersistentNode will make sure the ephemeral node created on server will be present
      // even under connection or session interruption (will automatically handle retries)
      znode = new PersistentNode(zooKeeperClient, CreateMode.EPHEMERAL_SEQUENTIAL, false,
          workersPath + "/" + workerNodePrefix, encoder.toBytes(srv));
    
      // start the creation of znodes
      znode.start();

      // We'll wait for 120s for node creation
      long znodeCreationTimeout = 120;
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception(
            "Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
      }

      znodePath = znode.getActualPath();

      if (doCheckAcls) {
        try {
          checkAndSetAcls();
        } catch (Exception ex) {
          throw new IOException("Error validating or setting ACLs. " + disableMessage, ex);
        }
      }
      if (zooKeeperClient.checkExists().forPath(znodePath) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode for this instance on ZooKeeper.");
      }
    } catch (Exception e) {
      LOG.error("Unable to create a znode for this server instance", e);
      CloseableUtils.closeQuietly(znode);
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }
    return uniqueId;
  }

  protected final void updateServiceRecord(
     ServiceRecord srv, boolean doCheckAcls, boolean closeOnFailure) throws IOException {
    if (srv.get(UNIQUE_IDENTIFIER) == null) {
      srv.set(UNIQUE_IDENTIFIER, UNIQUE_ID.toString());
    }
    // waitForInitialCreate must have already been called in registerServiceRecord.
    try {
      znode.setData(encoder.toBytes(srv));

      if (doCheckAcls) {
        try {
          checkAndSetAcls();
        } catch (Exception ex) {
          throw new IOException("Error validating or setting ACLs. " + disableMessage, ex);
        }
      }
    } catch (Exception e) {
      LOG.error("Unable to update znode with new service record", e);
      if (closeOnFailure) {
        CloseableUtils.closeQuietly(znode);
      }
      throw (e instanceof IOException) ? (IOException) e : new IOException(e);
    }
  }

  @VisibleForTesting
  public String getPersistentNodePath() {
    return "/" + PATH_JOINER.join(namespace, StringUtils.substringBetween(workersPath, "/", "/"), "pnode0");
  }

  protected void ensurePersistentNodePath(ServiceRecord srv) throws IOException {
    String pNodePath = getPersistentNodePath();
    try {
      LOG.info("Check if persistent node  path {}  exists, create if not", pNodePath);
      if (zooKeeperClient.checkExists().forPath(pNodePath) == null) {
        zooKeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
            .forPath(pNodePath, encoder.toBytes(srv));
        LOG.info("Created persistent path at: {}", pNodePath);
      }
    } catch (Exception e) {
      // throw exception if it is other than NODEEXISTS.
      if (!(e instanceof KeeperException) || ((KeeperException) e).code() != KeeperException.Code.NODEEXISTS) {
        LOG.error("Unable to create a persistent znode for this server instance", e);
        throw new IOException(e);
      } else {
        LOG.debug("Ignoring KeeperException while ensuring path as the parent node {} already exists.", pNodePath);
      }
    }
  }

  final protected void initializeWithoutRegisteringInternal() throws IOException {
    // Create a znode under the rootNamespace parent for this instance of the server
    try {
      try {
        zooKeeperClient.create().creatingParentsIfNeeded().forPath(workersPath);
      } catch (NodeExistsException ex) {
        // Ignore - this is expected.
      }
      if (doCheckAcls) {
        try {
          checkAndSetAcls();
        } catch (Exception ex) {
          throw new IOException("Error validating or setting ACLs. " + disableMessage, ex);
        }
      }
    } catch (Exception e) {
      LOG.error("Unable to create a parent znode for the registry", e);
      throw (e instanceof IOException) ? (IOException)e : new IOException(e);
    }
  }

  private void checkAndSetAcls() throws Exception {
    if (!ZookeeperUtils.isKerberosEnabled(conf)) {
      return;
    }
    // We are trying to check ACLs on the "workers" directory, which noone except us should be
    // able to write to. Higher-level directories shouldn't matter - we don't read them.
    String pathToCheck = workersPath;
    List<ACL> acls = zooKeeperClient.getACL().forPath(pathToCheck);
    if (acls == null || acls.isEmpty()) {
      // Can there be no ACLs? There's some access (to get ACLs), so assume it means free for all.
      LOG.warn("No ACLs on "  + pathToCheck + "; setting up ACLs. " + disableMessage);
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
        + "; setting up ACLs. " + disableMessage);
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

  private void addToCache(String path, String host, InstanceType instance) {
    instanceCacheLock.lock();
    try {
      putInInstanceCache(path, pathToInstanceCache, instance);
      putInNodeCache(host, nodeToInstanceCache, instance);
    } finally {
      instanceCacheLock.unlock();
    }
    LOG.debug("Added path={}, host={} instance={} to cache."
            + " pathToInstanceCache:size={}, nodeToInstanceCache:size={}",
        path, host, instance, pathToInstanceCache.size(), nodeToInstanceCache.size());
  }

  private void removeFromCache(String path, String host) {
    instanceCacheLock.lock();
    try {
      pathToInstanceCache.remove(path);
      nodeToInstanceCache.remove(host);
    } finally {
      instanceCacheLock.unlock();
    }
    LOG.debug("Removed path={}, host={} from cache."
            + " pathToInstanceCache:size={}, nodeToInstanceCache:size={}",
        path, host, pathToInstanceCache.size(), nodeToInstanceCache.size());
  }

  private void putInInstanceCache(String key, Map<String, InstanceType> cache,
      InstanceType instance) {
    cache.put(key, instance);
  }

  private void putInNodeCache(String key, Map<String, Set<InstanceType>> cache,
    InstanceType instance) {
    Set<InstanceType> instanceSet = cache.get(key);
    if (instanceSet == null) {
      instanceSet = new HashSet<>();
      instanceSet.add(instance);
    }
    cache.put(key, instanceSet);
  }

  protected final void populateCache(PathChildrenCache instancesCache, boolean doInvokeListeners) {
    for (ChildData childData : instancesCache.getCurrentData()) {
      byte[] data = getWorkerData(childData, workerNodePrefix);
      if (data == null) continue;
      String nodeName = extractNodeName(childData);
      if (!isLlapWorker(nodeName, workerNodePrefix)) continue;
      int ephSeqVersion = extractSeqNum(nodeName);
      try {
        ServiceRecord srv = encoder.fromBytes(childData.getPath(), data);
        InstanceType instance = createServiceInstance(srv);
        addToCache(childData.getPath(), instance.getHost(), instance);
        if (doInvokeListeners) {
          for (ServiceInstanceStateChangeListener<InstanceType> listener : stateChangeListeners) {
            listener.onCreate(instance, ephSeqVersion);
          }
        }
      } catch (IOException e) {
        LOG.error("Unable to decode data for zkpath: {}." +
            " Ignoring from current instances list..", childData.getPath());
      }
    }
  }

  private static boolean isLlapWorker(String nodeName, String workerNodePrefix) {
    return nodeName.startsWith(workerNodePrefix) && nodeName.length() > workerNodePrefix.length();
  }

  protected abstract InstanceType createServiceInstance(ServiceRecord srv) throws IOException;

  protected static byte[] getWorkerData(ChildData childData, String workerNodePrefix) {
    if (childData == null) return null;
    byte[] data = childData.getData();
    if (data == null) return null;
    if (!isLlapWorker(extractNodeName(childData), workerNodePrefix)) return null;
    return data;
  }

  private class InstanceStateChangeListener implements PathChildrenCacheListener {
    private final Logger LOG = LoggerFactory.getLogger(InstanceStateChangeListener.class);

    @Override
    public void childEvent(final CuratorFramework client, final PathChildrenCacheEvent event) {
      Preconditions.checkArgument(client != null
          && client.getState() == CuratorFrameworkState.STARTED, "client is not started");

      synchronized (this) {
        ChildData childData = event.getData();
        if (childData == null) return;
        String nodeName = extractNodeName(childData);
        if (nodeName.equals(workerNodePrefix)) {
          LOG.warn("Invalid LLAP worker node name: {} was {}", childData.getPath(), event.getType());
        }
        if (!isLlapWorker(nodeName, workerNodePrefix)) return;
        LOG.info("{} for zknode {}", event.getType(), childData.getPath());
        InstanceType instance = extractServiceInstance(event, childData);
        if (instance != null) {
          int ephSeqVersion = extractSeqNum(nodeName);
          switch (event.getType()) {
            case CHILD_ADDED:
              addToCache(childData.getPath(), instance.getHost(), instance);
              for (ServiceInstanceStateChangeListener<InstanceType> listener : stateChangeListeners) {
                listener.onCreate(instance, ephSeqVersion);
              }
              break;
            case CHILD_UPDATED:
              addToCache(childData.getPath(), instance.getHost(), instance);
              for (ServiceInstanceStateChangeListener<InstanceType> listener : stateChangeListeners) {
                listener.onUpdate(instance, ephSeqVersion);
              }
              break;
            case CHILD_REMOVED:
              removeFromCache(childData.getPath(), instance.getHost());
              for (ServiceInstanceStateChangeListener<InstanceType> listener : stateChangeListeners) {
                listener.onRemove(instance, ephSeqVersion);
              }
              break;
            default:
              // Ignore all the other events; logged above.
          }
        } else {
          LOG.info("instance is null for event: {} childData: {}", event.getType(), childData);
        }
      }
    }
  }

  // The real implementation for the instanceset... instanceset has its own copy of the
  // ZK cache yet completely depends on the parent in every other aspect and is thus unneeded.

  protected final int sizeInternal() {
    // not using the path child cache here as there could be more than 1 path per host (worker and slot znodes)
    return nodeToInstanceCache.size();
  }

  protected final Set<InstanceType> getByHostInternal(String host) {
    Set<InstanceType> byHost = nodeToInstanceCache.get(host);
    byHost = (byHost == null) ? Sets.newHashSet() : byHost;
    if (LOG.isDebugEnabled()) {
      LOG.debug("Returning " + byHost.size() + " hosts for locality allocation on " + host);
    }
    return byHost;
  }

  protected final Collection<InstanceType> getAllInternal() {
    return new HashSet<>(pathToInstanceCache.values());
  }

  private static String extractNodeName(ChildData childData) {
    String nodeName = childData.getPath();
    int ix = nodeName.lastIndexOf("/");
    if (ix >= 0) {
      nodeName = nodeName.substring(ix + 1);
    }
    return nodeName;
  }

  private InstanceType extractServiceInstance(
      PathChildrenCacheEvent event, ChildData childData) {
    byte[] data = childData.getData();
    if (data == null) return null;
    try {
      ServiceRecord srv = encoder.fromBytes(event.getData().getPath(), data);
      return createServiceInstance(srv);
    } catch (IOException e) {
      LOG.error("Unable to decode data for zknode: {}." +
          " Dropping notification of type: {}", childData.getPath(), event.getType());
      return null;
    }
  }

  public synchronized void registerStateChangeListener(
      ServiceInstanceStateChangeListener<InstanceType> listener) throws IOException {
    ensureInstancesCache(0);
    this.stateChangeListeners.add(listener);
  }

  @SuppressWarnings("resource") // Bogus warnings despite closeQuietly.
  protected final synchronized PathChildrenCache ensureInstancesCache(
      long clusterReadyTimeoutMs) throws IOException {
    Preconditions.checkArgument(zooKeeperClient != null &&
            zooKeeperClient.getState() == CuratorFrameworkState.STARTED, "client is not started");
    // lazily create PathChildrenCache
    PathChildrenCache instancesCache = this.instancesCache;
    if (instancesCache != null) return instancesCache;
    ExecutorService tp = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
              .setDaemon(true).setNameFormat("StateChangeNotificationHandler").build());
    long startTimeNs = System.nanoTime(), deltaNs = clusterReadyTimeoutMs * 1000000L;
    long sleepTimeMs = Math.min(16, clusterReadyTimeoutMs);
    while (true) {
      instancesCache = new PathChildrenCache(zooKeeperClient, workersPath, true);
      instancesCache.getListenable().addListener(new InstanceStateChangeListener(), tp);
      try {
        instancesCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        this.instancesCache = instancesCache;
        return instancesCache;
      } catch (InvalidACLException e) {
        // PathChildrenCache tried to mkdir when the znode wasn't there, and failed.
        CloseableUtils.closeQuietly(instancesCache);
        long elapsedNs = System.nanoTime() - startTimeNs;
        if (deltaNs == 0 || deltaNs <= elapsedNs) {
          LOG.error("Unable to start curator PathChildrenCache", e);
          throw new IOException(e);
        }
        LOG.warn("The cluster is not started yet (InvalidACL); will retry");
        try {
          Thread.sleep(Math.min(sleepTimeMs, (deltaNs - elapsedNs)/1000000L));
        } catch (InterruptedException e1) {
          LOG.error("Interrupted while retrying the PathChildrenCache startup");
          throw new IOException(e1);
        }
        sleepTimeMs = sleepTimeMs << 1;
      } catch (Exception e) {
        CloseableUtils.closeQuietly(instancesCache);
        LOG.error("Unable to start curator PathChildrenCache", e);
        throw new IOException(e);
      }
    }
  }

  public void start() throws IOException {
    if (zooKeeperClient != null) {
      String principal = ZookeeperUtils.setupZookeeperAuth(
          conf, saslLoginContextName, zkPrincipal, zkKeytab);
      if (principal != null) {
        userNameFromPrincipal = LlapUtil.getUserNameFromPrincipal(principal);
      }
      zooKeeperClient.start();
    }
    // Init closeable utils in case register is not called (see HIVE-13322)
    CloseableUtils.class.getName();
  }

  protected void unregisterInternal() {
    CloseableUtils.closeQuietly(znode);
  }

  public void stop() {
    CloseableUtils.closeQuietly(znode);
    CloseableUtils.closeQuietly(instancesCache);
    CloseableUtils.closeQuietly(zooKeeperClient);
  }

  protected final InstanceType getInstanceByPath(String path) {
    return pathToInstanceCache.get(path);
  }

  protected final String getRegistrationZnodePath() {
    return znodePath;
  }

  private int extractSeqNum(String nodeName) {
    // Extract the sequence number of this ephemeral-sequential znode.
    String ephSeqVersionStr = nodeName.substring(workerNodePrefix.length());
    try {
      return Integer.parseInt(ephSeqVersionStr);
    } catch (NumberFormatException e) {
      LOG.error("Cannot parse " + ephSeqVersionStr + " from " + nodeName, e);
      throw e;
    }
  }

  // for debugging
  private class ZkConnectionStateListener implements ConnectionStateListener {
    @Override
    public void stateChanged(final CuratorFramework curatorFramework, final ConnectionState connectionState) {
      LOG.info("Connection state change notification received. State: {}", connectionState);
    }
  }

  public String currentUser() {
    try {
      return UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
