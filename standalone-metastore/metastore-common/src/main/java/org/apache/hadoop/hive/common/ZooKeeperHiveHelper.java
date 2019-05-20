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

package org.apache.hadoop.hive.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.List;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// The class serves three purposes (for HiveServer2 and HiveMetaStore)
// 1. An instance of this class holds ZooKeeper related configuration parameter values from Hive
// configuration and metastore configuration.
// 2. For a server which is added to ZooKeeper specified by the configuration, an instance of
// this class holds the znode corresponding to that server, zookeeper client used to watch the
// znode.
// 3. For a metastore client it provides API to find server URIs from specified ZooKeeper.
//
// We could have differentiated these three functionality into three different classes by
// including an instance of first class in the second and the third, but there's isn't much stuff
// in the first and the third.. Also note that the third functionality overlaps with
// ZooKeeperHiveClientHelper class, but that overlap is very small. So for now all the three
// functionality are bundled in a single class.

/**
 * ZooKeeperHiveHelper. A helper class to hold ZooKeeper related configuration, to register and
 * deregister ZooKeeper node for a given server and to fetch registered server URIs for clients.
 */
public class ZooKeeperHiveHelper {
  public static final Logger LOG = LoggerFactory.getLogger(ZooKeeperHiveHelper.class.getName());
  public static final String ZOOKEEPER_PATH_SEPARATOR = "/";

  private String quorum = null;
  private String clientPort = null;
  private String rootNamespace = null;
  private boolean deregisteredWithZooKeeper = false; // Set to true only when deregistration happens
  private int sessionTimeout;
  private int baseSleepTime;
  private int maxRetries;

  private CuratorFramework zooKeeperClient;
  private PersistentEphemeralNode znode;

  public ZooKeeperHiveHelper(String quorum, String clientPort, String rootNamespace,
                             int sessionTimeout, int baseSleepTime, int maxRetries) {
    // Get the ensemble server addresses in the format host1:port1, host2:port2, ... . Append
    // the configured port to hostname if the hostname doesn't contain a port.
    String[] hosts = quorum.split(",");
    StringBuilder quorumServers = new StringBuilder();
    for (int i = 0; i < hosts.length; i++) {
      quorumServers.append(hosts[i].trim());
      if (!hosts[i].contains(":")) {
        quorumServers.append(":");
        quorumServers.append(clientPort);
      }

      if (i != hosts.length - 1) {
        quorumServers.append(",");
      }
    }

    this.quorum = quorumServers.toString();
    this.clientPort = clientPort;
    this.rootNamespace = rootNamespace;
    this.sessionTimeout = sessionTimeout;
    this.baseSleepTime = baseSleepTime;
    this.maxRetries = maxRetries;
  }

  /**
   * Get the ensemble server addresses. The format is: host1:port, host2:port..
   **/
  public String getQuorumServers() {
    return quorum;
  }

  /**
   * Adds a server instance to ZooKeeper as a znode.
   *
   * @throws Exception
   */
  public void addServerInstanceToZooKeeper(String znodePathPrefix, String znodeData,
                                           ACLProvider zooKeeperAclProvider,
                                           ZKDeRegisterWatcher watcher) throws Exception {
    // This might be the first server getting added to the ZooKeeper, so the parent node may need
    // to be created.
    zooKeeperClient = startZookeeperClient(zooKeeperAclProvider, true);

    // Create a znode under the rootNamespace parent for the given path prefix for a server. Also
    // add a watcher to watch the znode.
    try {
      String pathPrefix = ZOOKEEPER_PATH_SEPARATOR + rootNamespace
                      + ZOOKEEPER_PATH_SEPARATOR + znodePathPrefix;
      byte[] znodeDataUTF8 = znodeData.getBytes(StandardCharsets.UTF_8);
      znode =
              new PersistentEphemeralNode(zooKeeperClient,
                      PersistentEphemeralNode.Mode.EPHEMERAL_SEQUENTIAL, pathPrefix, znodeDataUTF8);
      znode.start();
      // We'll wait for 120s for node creation
      long znodeCreationTimeout = 120;
      if (!znode.waitForInitialCreate(znodeCreationTimeout, TimeUnit.SECONDS)) {
        throw new Exception("Max znode creation wait time: " + znodeCreationTimeout + "s exhausted");
      }
      setDeregisteredWithZooKeeper(false);
      final String znodePath = znode.getActualPath();
      if (zooKeeperClient.checkExists().usingWatcher(watcher).forPath(znodePath) == null) {
        // No node exists, throw exception
        throw new Exception("Unable to create znode with path prefix " + znodePathPrefix +
                " and data " + znodeData + " on ZooKeeper.");
      }
      LOG.info("Created a znode (actual path " + znodePath + ") on ZooKeeper with path prefix " +
                      znodePathPrefix + " and data " + znodeData);
    } catch (Exception e) {
      LOG.error("Unable to create znode with path prefix " + znodePathPrefix + " and data " +
                znodeData + " on ZooKeeper.", e);
      if (znode != null) {
        znode.close();
      }
      throw (e);
    }
  }

  public CuratorFramework startZookeeperClient(ACLProvider zooKeeperAclProvider,
                                               boolean addParentNode) throws Exception {
    String zooKeeperEnsemble = getQuorumServers();
    // Create a CuratorFramework instance to be used as the ZooKeeper client.
    // Use the zooKeeperAclProvider, when specified, to create appropriate ACLs.
    CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
            .connectString(zooKeeperEnsemble)
            .sessionTimeoutMs(sessionTimeout)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries));
    if (zooKeeperAclProvider != null) {
      builder = builder.aclProvider(zooKeeperAclProvider);
    }
    CuratorFramework zkClient = builder.build();
    zkClient.start();

    // Create the parent znodes recursively; ignore if the parent already exists.
    if (addParentNode) {
      try {
        zkClient.create()
                .creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath(ZooKeeperHiveHelper.ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
        LOG.info("Created the root name space: " + rootNamespace + " on ZooKeeper");
      } catch (KeeperException e) {
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          LOG.error("Unable to create namespace: " + rootNamespace + " on ZooKeeper", e);
          throw e;
        }
      }
    }
    return zkClient;
  }

  public void removeServerInstanceFromZooKeeper() throws Exception {
    setDeregisteredWithZooKeeper(true);

    if (znode != null) {
      znode.close();
      znode = null;
    }
    zooKeeperClient.close();
    LOG.info("Server instance removed from ZooKeeper.");
  }


  public void deregisterZnode() {
    if (znode != null) {
      try {
        znode.close();
        LOG.warn("This server instance with path " + znode.getActualPath() +
                " is now de-registered from ZooKeeper. ");
      } catch (IOException e) {
        LOG.error("Failed to close the persistent ephemeral znode", e);
      } finally {
        setDeregisteredWithZooKeeper(true);
        znode = null;
      }
    }
  }

  public boolean isDeregisteredWithZooKeeper() {
    return deregisteredWithZooKeeper;
  }

  private void setDeregisteredWithZooKeeper(boolean deregisteredWithZooKeeper) {
    this.deregisteredWithZooKeeper = deregisteredWithZooKeeper;
  }

  /**
   * This method is supposed to be called from client code connecting to one of the servers
   * managed by the configured ZooKeeper. It starts and closes its own ZooKeeper client instead
   * of using the class member.
   * @return list of server URIs stored under the configured zookeeper namespace
   * @throws Exception
   */
  public List<String> getServerUris() throws Exception {
    CuratorFramework zkClient = null;
    List<String> serverUris;
    try {
      zkClient = startZookeeperClient(null, false);
      List<String> serverNodes =
              zkClient.getChildren().forPath(ZOOKEEPER_PATH_SEPARATOR + rootNamespace);
      serverUris = new ArrayList<String>(serverNodes.size());
      for (String serverNode : serverNodes) {
        byte[] serverUriBytes = zkClient.getData()
                .forPath(ZOOKEEPER_PATH_SEPARATOR + rootNamespace +
                        ZOOKEEPER_PATH_SEPARATOR + serverNode);
        serverUris.add(new String(serverUriBytes, StandardCharsets.UTF_8));
      }
      zkClient.close();
      return serverUris;
    } catch (Exception e) {
      if (zkClient != null) {
        zkClient.close();
      }
      throw e;
    }
  }
}
