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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.registry.impl.ZookeeperUtils;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Kill query coordination service.
 * When service discovery is enabled a local kill query can request a kill
 * on every other HS2 server with the queryId or queryTag and wait for confirmation on denial.
 * The communication is done through Zookeeper.
 */
public class KillQueryZookeeperManager extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(KillQueryZookeeperManager.class);
  private static final String SASL_LOGIN_CONTEXT_NAME = "KillQueryZooKeeperClient";
  public static final int MAX_WAIT_ON_CONFIRMATION_SECONDS = 30;
  public static final int MAX_WAIT_ON_KILL_SECONDS = 180;

  private CuratorFramework zooKeeperClient;
  private String zkPrincipal, zkKeytab, zkNameSpace;
  private final KillQueryImpl localKillQueryImpl;
  private final HiveServer2 hiveServer2;
  private HiveConf conf;

  // Path cache to watch queries to kill
  private PathChildrenCache killQueryListener = null;

  public KillQueryZookeeperManager(OperationManager operationManager, HiveServer2 hiveServer2) {
    super(KillQueryZookeeperManager.class.getSimpleName());
    this.hiveServer2 = hiveServer2;
    localKillQueryImpl = new KillQueryImpl(operationManager, this);
  }

  @Override
  public synchronized void init(HiveConf conf) {
    this.conf = conf;
    zkNameSpace = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_KILLQUERY_NAMESPACE);
    Preconditions.checkArgument(!StringUtils.isBlank(zkNameSpace),
        HiveConf.ConfVars.HIVE_ZOOKEEPER_KILLQUERY_NAMESPACE.varname + " cannot be null or empty");
    this.zkPrincipal = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    this.zkKeytab = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    this.zooKeeperClient = conf.getZKConfig().getNewZookeeperClient(getACLProviderForZKPath("/" + zkNameSpace));
    this.zooKeeperClient.getConnectionStateListenable().addListener(new ZkConnectionStateListener());

    super.init(conf);
  }

  @Override
  public synchronized void start() {
    super.start();
    if (zooKeeperClient == null) {
      throw new ServiceException("Failed start zookeeperClient in KillQueryZookeeperManager");
    }
    try {
      ZookeeperUtils.setupZookeeperAuth(this.getHiveConf(), SASL_LOGIN_CONTEXT_NAME, zkPrincipal, zkKeytab);
      zooKeeperClient.start();
      try {
        zooKeeperClient.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/" + zkNameSpace);
        if (ZookeeperUtils.isKerberosEnabled(conf)) {
          zooKeeperClient.setACL().withACL(createSecureAcls()).forPath("/" + zkNameSpace);
        }
        LOG.info("Created the root namespace: " + zkNameSpace + " on ZooKeeper");
      } catch (KeeperException e) {
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          LOG.error("Unable to create namespace: " + zkNameSpace + " on ZooKeeper", e);
          throw e;
        }
      }
      // Create a path cache and start to listen for every kill query request from other servers.
      killQueryListener = new PathChildrenCache(zooKeeperClient, "/" + zkNameSpace, false);
      killQueryListener.start(PathChildrenCache.StartMode.NORMAL);
      startListeningForQueries();
      // Init closeable utils in case register is not called (see HIVE-13322)
      CloseableUtils.class.getName();
    } catch (Exception e) {
      throw new RuntimeException("Failed start zookeeperClient in KillQueryZookeeperManager", e);
    }
    LOG.info("KillQueryZookeeperManager service started.");
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

  private static List<ACL> createSecureAcls() {
    // Read all to the world
    List<ACL> nodeAcls = new ArrayList<>(ZooDefs.Ids.READ_ACL_UNSAFE);
    // Create/Delete/Write/Admin to creator
    nodeAcls.addAll(ZooDefs.Ids.CREATOR_ALL_ACL);
    return nodeAcls;
  }

  private void startListeningForQueries() {
    PathChildrenCacheListener listener = (client, pathChildrenCacheEvent) -> {
      if (pathChildrenCacheEvent.getType() == PathChildrenCacheEvent.Type.CHILD_ADDED) {
        KillQueryZookeeperBarrier barrier = new KillQueryZookeeperBarrier(zooKeeperClient, "/" + zkNameSpace,
            ZKPaths.getNodeFromPath(pathChildrenCacheEvent.getData().getPath()));
        Optional<KillQueryZookeeperData> data = barrier.getKillQueryData();
        if (!data.isPresent()) {
          return;
        }
        KillQueryZookeeperData killQuery = data.get();
        LOG.debug("Kill query request with id {}", killQuery.getQueryId());
        if (getServerHost().equals(killQuery.getRequestingServer())) {
          // The listener was called for the server who posted the request
          return;
        }
        if (localKillQueryImpl.isLocalQuery(killQuery.getQueryId())) {
          LOG.info("Killing query with id {}", killQuery.getQueryId());
          barrier.confirmProgress(getServerHost());
          try {
            localKillQueryImpl
                .killLocalQuery(killQuery.getQueryId(), conf, killQuery.getDoAs(), killQuery.isDoAsAdmin());
            barrier.confirmDone(getServerHost());
          } catch (Exception e) {
            LOG.error("Unable to kill local query", e);
            barrier.confirmFailed(getServerHost());
          }
        } else {
          LOG.debug("Confirm unknown kill query request with id {}", killQuery.getQueryId());
          barrier.confirmNo(getServerHost());
        }
      }
    };
    LOG.info("Start to listen for kill query requests.");
    killQueryListener.getListenable().addListener(listener);
  }

  @Override
  public synchronized void stop() {
    super.stop();
    LOG.info("Stopping KillQueryZookeeperManager service.");
    CloseableUtils.closeQuietly(killQueryListener);
    CloseableUtils.closeQuietly(zooKeeperClient);
  }

  private List<String> getAllServerUrls() {
    List<String> serverHosts = new ArrayList<>();
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY) && !conf
        .getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE)) {
      String zooKeeperNamespace = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE);
      try {
        serverHosts.addAll(zooKeeperClient.getChildren().forPath("/" + zooKeeperNamespace));
      } catch (Exception e) {
        LOG.error("Unable the get available server hosts", e);
      }
    }
    return serverHosts;
  }

  private String getServerHost() {
    if (hiveServer2 == null) {
      return "";
    }
    try {
      return removeDelimiter(hiveServer2.getServerInstanceURI());
    } catch (Exception e) {
      LOG.error("Unable to determine the server host", e);
      return "";
    }
  }

  // for debugging
  private static class ZkConnectionStateListener implements ConnectionStateListener {
    @Override
    public void stateChanged(final CuratorFramework curatorFramework, final ConnectionState connectionState) {
      LOG.info("Connection state change notification received. State: {}", connectionState);
    }
  }

  /**
   * Post a kill query request on Zookeeper for the other HS2 instances. If the service discovery is not enabled or
   * there is no other server registered, does nothing. Otherwise post the kill query request on Zookeeper
   * and waits for the other instances to confirm the kill or deny it.
   *
   * @param queryIdOrTag queryId or tag to kill
   * @param doAs         user requesting the kill
   * @param doAsAdmin    admin user requesting the kill (with KILLQUERY privilege)
   * @throws IOException If the kill query failed
   */
  public void killQuery(String queryIdOrTag, String doAs, boolean doAsAdmin) throws IOException {
    List<String> serverHosts = getAllServerUrls();
    if (serverHosts.size() < 2) {
      return;
    }
    KillQueryZookeeperBarrier barrier = new KillQueryZookeeperBarrier(zooKeeperClient, "/" + zkNameSpace);
    boolean result;
    try {
      barrier.setBarrier(queryIdOrTag, hiveServer2.getServerInstanceURI(), doAs, doAsAdmin);
      LOG.info("Created kill query barrier in path: {} for queryId: {}", barrier.getBarrierPath(), queryIdOrTag);
      result = barrier.waitOnBarrier(serverHosts.size() - 1, MAX_WAIT_ON_CONFIRMATION_SECONDS,
          MAX_WAIT_ON_KILL_SECONDS, TimeUnit.SECONDS);

    } catch (Exception e) {
      LOG.error("Unable to create Barrier on Zookeeper for KillQuery", e);
      throw new IOException(e);
    }
    if (!result) {
      throw new IOException("Unable to kill query on remote servers");
    }
  }

  /**
   * Data to post to Zookeeper for a kill query request. The fields will be serialized with ':' delimiter.
   * In requestingServer every ':' will be escaped. Other fields can not contain any ':'.
   */
  public static class KillQueryZookeeperData {
    private String queryId;
    private String requestingServer;
    private String doAs;
    private boolean doAsAdmin;

    public KillQueryZookeeperData(String queryId, String requestingServer, String doAs, boolean doAsAdmin) {
      if (!StringUtils.equals(queryId, removeDelimiter(queryId))) {
        throw new IllegalArgumentException("QueryId can not contain any ':' character.");
      }
      this.queryId = queryId;
      this.requestingServer = removeDelimiter(requestingServer);
      if (!StringUtils.equals(doAs, removeDelimiter(doAs))) {
        throw new IllegalArgumentException("doAs can not contain any ':' character.");
      }
      this.doAs = doAs;
      this.doAsAdmin = doAsAdmin;
    }

    public KillQueryZookeeperData(String data) {
      if (data == null) {
        return;
      }

      String[] elem = data.split(":");
      queryId = elem[0];
      requestingServer = elem[1];
      doAs = elem[2];
      doAsAdmin = Boolean.parseBoolean(elem[3]);
    }

    @Override
    public String toString() {
      return queryId + ":" + requestingServer + ":" + doAs + ":" + doAsAdmin;
    }

    public String getQueryId() {
      return queryId;
    }

    public String getRequestingServer() {
      return requestingServer;
    }

    public String getDoAs() {
      return doAs;
    }

    public boolean isDoAsAdmin() {
      return doAsAdmin;
    }
  }

  /**
   * Zookeeper Barrier for the KillQuery Operation.
   * It post a kill query request on Zookeeper and waits until the given number of service instances responses.
   * Implementation is based on org.apache.curator.framework.recipes.barriers.DistributedBarrier.
   */
  public static class KillQueryZookeeperBarrier {
    private final CuratorFramework client;
    private final String barrierPath;
    private final Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        client.postSafeNotify(KillQueryZookeeperBarrier.this);
      }
    };

    /**
     * @param client          client
     * @param barrierRootPath rootPath to put the barrier
     */
    public KillQueryZookeeperBarrier(CuratorFramework client, String barrierRootPath) {
      this(client, barrierRootPath, UUID.randomUUID().toString());
    }

    /**
     * @param client          client
     * @param barrierRootPath rootPath to put the barrier
     * @param barrierPath     name of the barrier
     */
    public KillQueryZookeeperBarrier(CuratorFramework client, String barrierRootPath, String barrierPath) {
      this.client = client;
      this.barrierPath = PathUtils.validatePath(barrierRootPath + "/" + barrierPath);
    }

    public String getBarrierPath() {
      return barrierPath;
    }

    /**
     * Utility to set the barrier node.
     *
     * @throws Exception errors
     */
    public synchronized void setBarrier(String queryId, String requestingServer, String doAs, boolean doAsAdmin)
        throws Exception {
      try {
        KillQueryZookeeperData data = new KillQueryZookeeperData(queryId, requestingServer, doAs, doAsAdmin);
        client.create().creatingParentContainersIfNeeded()
            .forPath(barrierPath, data.toString().getBytes(StandardCharsets.UTF_8));
      } catch (KeeperException.NodeExistsException e) {
        throw new IllegalStateException("Barrier with this path already exists");
      }
    }

    public synchronized Optional<KillQueryZookeeperData> getKillQueryData() throws Exception {
      if (client.checkExists().forPath(barrierPath) != null) {
        byte[] data = client.getData().forPath(barrierPath);
        return Optional.of(new KillQueryZookeeperData(new String(data, StandardCharsets.UTF_8)));
      }
      return Optional.empty();
    }

    /**
     * Confirm not knowing the query with the queryId in the barrier.
     *
     * @param serverId The serverHost confirming the request
     * @throws Exception If confirmation failed
     */
    public synchronized void confirmNo(String serverId) throws Exception {
      if (client.checkExists().forPath(barrierPath) != null) {
        client.create().forPath(barrierPath + "/NO:" + serverId);
      } else {
        throw new IllegalStateException("Barrier is not initialised");
      }
    }

    /**
     * Confirm knowing the query with the queryId in the barrier and starting the kill query process.
     *
     * @param serverId The serverHost confirming the request
     * @throws Exception If confirmation failed
     */
    public synchronized void confirmProgress(String serverId) throws Exception {
      if (client.checkExists().forPath(barrierPath) != null) {
        client.create().forPath(barrierPath + "/PROGRESS:" + serverId);
      } else {
        throw new IllegalStateException("Barrier is not initialised");
      }
    }

    /**
     * Confirm killing the query with the queryId in the barrier.
     *
     * @param serverId The serverHost confirming the request
     * @throws Exception If confirmation failed
     */
    public synchronized void confirmDone(String serverId) throws Exception {
      if (client.checkExists().forPath(barrierPath) != null) {
        if (client.checkExists().forPath(barrierPath + "/PROGRESS:" + serverId) != null) {
          client.delete().forPath(barrierPath + "/PROGRESS:" + serverId);
        }
        client.create().forPath(barrierPath + "/DONE:" + serverId);
      } else {
        throw new IllegalStateException("Barrier is not initialised");
      }
    }

    /**
     * Confirm failure of killing the query with the queryId in the barrier.
     *
     * @param serverId The serverHost confirming the request
     * @throws Exception If confirmation failed
     */
    public synchronized void confirmFailed(String serverId) throws Exception {
      if (client.checkExists().forPath(barrierPath) != null) {
        if (client.checkExists().forPath(barrierPath + "/PROGRESS:" + serverId) != null) {
          client.delete().forPath(barrierPath + "/PROGRESS:" + serverId);
        }
        client.create().forPath(barrierPath + "/FAILED:" + serverId);
      } else {
        throw new IllegalStateException("Barrier is not initialised");
      }
    }

    /**
     * Wait for every server either confirm killing the query or confirm not knowing the query.
     *
     * @param confirmationCount     number of confirmation to wait for
     * @param maxWaitOnConfirmation confirmation waiting timeout for NO answers
     * @param maxWaitOnKill         timeout for waiting on the actual kill query operation
     * @param unit                  time unit for timeouts
     * @return true if the kill was confirmed, false on timeout or if everybody voted for NO
     * @throws Exception If confirmation failed
     */
    public synchronized boolean waitOnBarrier(int confirmationCount, long maxWaitOnConfirmation, long maxWaitOnKill,
        TimeUnit unit) throws Exception {
      long startMs = System.currentTimeMillis();
      long startKill = -1;
      long maxWaitMs = TimeUnit.MILLISECONDS.convert(maxWaitOnConfirmation, unit);
      long maxWaitOnKillMs = TimeUnit.MILLISECONDS.convert(maxWaitOnKill, unit);

      boolean progress = false;
      boolean result = false;
      while (true) {
        List<String> children = client.getChildren().usingWatcher(watcher).forPath(barrierPath);
        boolean concluded = false;
        for (String child : children) {
          if (child.startsWith("DONE")) {
            result = true;
            concluded = true;
            break;
          }
          if (child.startsWith("FAILED")) {
            concluded = true;
            break;
          }
          if (child.startsWith("PROGRESS")) {
            progress = true;
          }
        }
        if (concluded) {
          break;
        }
        if (progress) {
          // Wait for the kill query to finish
          if (startKill < 0) {
            startKill = System.currentTimeMillis();
          }
          long elapsed = System.currentTimeMillis() - startKill;
          long thisWaitMs = maxWaitOnKillMs - elapsed;
          if (thisWaitMs <= 0) {
            break;
          }
          wait(thisWaitMs);
        } else {
          if (children.size() == confirmationCount) {
            result = false;
            break;
          }
          // Wait for confirmation
          long elapsed = System.currentTimeMillis() - startMs;
          long thisWaitMs = maxWaitMs - elapsed;
          if (thisWaitMs <= 0) {
            break;
          }
          wait(thisWaitMs);
        }

      }
      client.delete().deletingChildrenIfNeeded().forPath(barrierPath);
      return result;
    }
  }

  private static String removeDelimiter(String in) {
    if (in == null) {
      return null;
    }
    return in.replaceAll(":", "");
  }
}
