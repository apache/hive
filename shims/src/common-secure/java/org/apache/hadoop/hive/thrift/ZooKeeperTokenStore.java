/**
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

package org.apache.hadoop.hive.thrift;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.HiveDelegationTokenSupport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper token store implementation.
 */
public class ZooKeeperTokenStore implements DelegationTokenStore {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ZooKeeperTokenStore.class.getName());

  protected static final String ZK_SEQ_FORMAT = "%010d";
  private static final String NODE_KEYS = "/keys";
  private static final String NODE_TOKENS = "/tokens";

  private String rootNode = "";
  private volatile ZooKeeper zkSession;
  private String zkConnectString;
  private final int zkSessionTimeout = 3000;
  private long connectTimeoutMillis = -1;
  private List<ACL> newNodeAcl = Ids.OPEN_ACL_UNSAFE;

  private class ZooKeeperWatcher implements Watcher {
    public void process(org.apache.zookeeper.WatchedEvent event) {
      LOGGER.info(event.toString());
      if (event.getState() == Watcher.Event.KeeperState.Expired) {
        LOGGER.warn("ZooKeeper session expired, discarding connection");
        try {
          zkSession.close();
        } catch (Throwable e) {
          LOGGER.warn("Failed to close connection on expired session", e);
        }
      }
    }
    
  }

  /**
   * Default constructor for dynamic instantiation w/ Configurable
   * (ReflectionUtils does not support Configuration constructor injection).
   */
  protected ZooKeeperTokenStore() {
  }

  public ZooKeeperTokenStore(String hostPort) {
    this.zkConnectString = hostPort;
    init();
  }

  private ZooKeeper getSession() {
    if (zkSession == null || zkSession.getState() == States.CLOSED) {
        synchronized (this) {
          if (zkSession == null || zkSession.getState() == States.CLOSED) {
            try {
              zkSession = createConnectedClient(this.zkConnectString, this.zkSessionTimeout,
                this.connectTimeoutMillis, new ZooKeeperWatcher());
            } catch (IOException ex) {
              throw new TokenStoreException("Token store error.", ex);
            }
          }
        }
    }
    return zkSession;
  }

  /**
   * Create a ZooKeeper session that is in connected state.
   * 
   * @param connectString ZooKeeper connect String
   * @param sessionTimeout ZooKeeper session timeout
   * @param connectTimeout milliseconds to wait for connection, 0 or negative value means no wait
   * @param watchers
   * @return
   * @throws InterruptedException
   * @throws IOException
   */
  public static ZooKeeper createConnectedClient(String connectString,
      int sessionTimeout, long connectTimeout, final Watcher... watchers)
      throws IOException {
    final CountDownLatch connected = new CountDownLatch(1);
    Watcher connectWatcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        switch (event.getState()) {
        case SyncConnected:
          connected.countDown();
          break;
        }
        for (Watcher w : watchers) {
          w.process(event);
        }
      }
    };
    ZooKeeper zk = new ZooKeeper(connectString, sessionTimeout, connectWatcher);
    if (connectTimeout > 0) {
      try {
        if (!connected.await(connectTimeout, TimeUnit.MILLISECONDS)) {
          zk.close();
          throw new IOException("Timeout waiting for connection after "
              + connectTimeout + "ms");
        }
      } catch (InterruptedException e) {
        throw new IOException("Error waiting for connection.", e);
      }
    }
    return zk;
  }
  
  /**
   * Create a path if it does not already exist ("mkdir -p")
   * @param zk ZooKeeper session
   * @param path string with '/' separator
   * @param acl list of ACL entries
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public static String ensurePath(ZooKeeper zk, String path, List<ACL> acl) throws KeeperException,
      InterruptedException {
    String[] pathComps = StringUtils.splitByWholeSeparator(path, "/");
    String currentPath = "";
    for (String pathComp : pathComps) {
      currentPath += "/" + pathComp;
      try {
        String node = zk.create(currentPath, new byte[0], acl,
            CreateMode.PERSISTENT);
        LOGGER.info("Created path: " + node);
      } catch (KeeperException.NodeExistsException e) {
      }
    }
    return currentPath;
  }

  /**
   * Parse ACL permission string, from ZooKeeperMain private method
   * @param permString
   * @return
   */
  public static int getPermFromString(String permString) {
      int perm = 0;
      for (int i = 0; i < permString.length(); i++) {
          switch (permString.charAt(i)) {
          case 'r':
              perm |= ZooDefs.Perms.READ;
              break;
          case 'w':
              perm |= ZooDefs.Perms.WRITE;
              break;
          case 'c':
              perm |= ZooDefs.Perms.CREATE;
              break;
          case 'd':
              perm |= ZooDefs.Perms.DELETE;
              break;
          case 'a':
              perm |= ZooDefs.Perms.ADMIN;
              break;
          default:
              LOGGER.error("Unknown perm type: " + permString.charAt(i));
          }
      }
      return perm;
  }

  /**
   * Parse comma separated list of ACL entries to secure generated nodes, e.g.
   * <code>sasl:hive/host1@MY.DOMAIN:cdrwa,sasl:hive/host2@MY.DOMAIN:cdrwa</code>
   * @param aclString
   * @return ACL list
   */
  public static List<ACL> parseACLs(String aclString) {
    String[] aclComps = StringUtils.splitByWholeSeparator(aclString, ",");
    List<ACL> acl = new ArrayList<ACL>(aclComps.length);
    for (String a : aclComps) {
      if (StringUtils.isBlank(a)) {
         continue;
      }
      a = a.trim();
      // from ZooKeeperMain private method
      int firstColon = a.indexOf(':');
      int lastColon = a.lastIndexOf(':');
      if (firstColon == -1 || lastColon == -1 || firstColon == lastColon) {
         LOGGER.error(a + " does not have the form scheme:id:perm");
         continue;
      }
      ACL newAcl = new ACL();
      newAcl.setId(new Id(a.substring(0, firstColon), a.substring(
          firstColon + 1, lastColon)));
      newAcl.setPerms(getPermFromString(a.substring(lastColon + 1)));
      acl.add(newAcl);
    }
    return acl;
  }

  private void init() {
    if (this.zkConnectString == null) {
      throw new IllegalStateException("Not initialized");
    }

    if (this.zkSession != null) {
      try {
        this.zkSession.close();
      } catch (InterruptedException ex) {
        LOGGER.warn("Failed to close existing session.", ex);
      }
    }
    ZooKeeper zk = getSession();

    try {
        ensurePath(zk, rootNode + NODE_KEYS, newNodeAcl);
        ensurePath(zk, rootNode + NODE_TOKENS, newNodeAcl);
      } catch (Exception e) {
        throw new TokenStoreException("Failed to validate token path.", e);
      }
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf == null) {
       throw new IllegalArgumentException("conf is null");
    }
    this.zkConnectString = conf.get(
      HadoopThriftAuthBridge20S.Server.DELEGATION_TOKEN_STORE_ZK_CONNECT_STR, null);
    this.connectTimeoutMillis = conf.getLong(
      HadoopThriftAuthBridge20S.Server.DELEGATION_TOKEN_STORE_ZK_CONNECT_TIMEOUTMILLIS, -1);
    this.rootNode = conf.get(
      HadoopThriftAuthBridge20S.Server.DELEGATION_TOKEN_STORE_ZK_ZNODE,
      HadoopThriftAuthBridge20S.Server.DELEGATION_TOKEN_STORE_ZK_ZNODE_DEFAULT);
    String csv = conf.get(HadoopThriftAuthBridge20S.Server.DELEGATION_TOKEN_STORE_ZK_ACL, null);
    if (StringUtils.isNotBlank(csv)) {
       this.newNodeAcl = parseACLs(csv);
    }
    init();
  }

  @Override
  public Configuration getConf() {
    return null; // not required
  }

  private Map<Integer, byte[]> getAllKeys() throws KeeperException,
      InterruptedException {

    String masterKeyNode = rootNode + NODE_KEYS;
    ZooKeeper zk = getSession();
    List<String> nodes = zk.getChildren(masterKeyNode, false);
    Map<Integer, byte[]> result = new HashMap<Integer, byte[]>();
    for (String node : nodes) {
      byte[] data = zk.getData(masterKeyNode + "/" + node, false, null);
      if (data != null) {
        result.put(getSeq(node), data);
      }
    }
    return result;
  }

  private int getSeq(String path) {
    String[] pathComps = path.split("/");
    return Integer.parseInt(pathComps[pathComps.length-1]);
  }

  @Override
  public int addMasterKey(String s) {
    try {
      ZooKeeper zk = getSession();
      String newNode = zk.create(rootNode + NODE_KEYS + "/", s.getBytes(), newNodeAcl,
          CreateMode.PERSISTENT_SEQUENTIAL);
      LOGGER.info("Added key {}", newNode);
      return getSeq(newNode);
    } catch (KeeperException ex) {
      throw new TokenStoreException(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreException(ex);
    }
  }

  @Override
  public void updateMasterKey(int keySeq, String s) {
    try {
      ZooKeeper zk = getSession();
      zk.setData(rootNode + NODE_KEYS + "/" + String.format(ZK_SEQ_FORMAT, keySeq), s.getBytes(),
          -1);
    } catch (KeeperException ex) {
      throw new TokenStoreException(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreException(ex);
    }
  }

  @Override
  public boolean removeMasterKey(int keySeq) {
    try {
      ZooKeeper zk = getSession();
      zk.delete(rootNode + NODE_KEYS + "/" + String.format(ZK_SEQ_FORMAT, keySeq), -1);
      return true;
    } catch (KeeperException.NoNodeException ex) {
      return false;
    } catch (KeeperException ex) {
      throw new TokenStoreException(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreException(ex);
    }
  }

  @Override
  public String[] getMasterKeys() {
    try {
      Map<Integer, byte[]> allKeys = getAllKeys();
      String[] result = new String[allKeys.size()];
      int resultIdx = 0;
      for (byte[] keyBytes : allKeys.values()) {
          result[resultIdx++] = new String(keyBytes);
      }
      return result;
    } catch (KeeperException ex) {
      throw new TokenStoreException(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreException(ex);
    }
  }


  private String getTokenPath(DelegationTokenIdentifier tokenIdentifier) {
    try {
      return rootNode + NODE_TOKENS + "/"
          + TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier);
    } catch (IOException ex) {
      throw new TokenStoreException("Failed to encode token identifier", ex);
    }
  }

  @Override
  public boolean addToken(DelegationTokenIdentifier tokenIdentifier,
      DelegationTokenInformation token) {
    try {
      ZooKeeper zk = getSession();
      byte[] tokenBytes = HiveDelegationTokenSupport.encodeDelegationTokenInformation(token);
      String newNode = zk.create(getTokenPath(tokenIdentifier),
          tokenBytes, newNodeAcl, CreateMode.PERSISTENT);
      LOGGER.info("Added token: {}", newNode);
      return true;
    } catch (KeeperException.NodeExistsException ex) {
      return false;
    } catch (KeeperException ex) {
      throw new TokenStoreException(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreException(ex);
    }
  }

  @Override
  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) {
    try {
      ZooKeeper zk = getSession();
      zk.delete(getTokenPath(tokenIdentifier), -1);
      return true;
    } catch (KeeperException.NoNodeException ex) {
      return false;
    } catch (KeeperException ex) {
      throw new TokenStoreException(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreException(ex);
    }
  }

  @Override
  public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier) {
    try {
      ZooKeeper zk = getSession();
      byte[] tokenBytes = zk.getData(getTokenPath(tokenIdentifier), false, null);
      try {
        return HiveDelegationTokenSupport.decodeDelegationTokenInformation(tokenBytes);
      } catch (Exception ex) {
        throw new TokenStoreException("Failed to decode token", ex);
      }
    } catch (KeeperException.NoNodeException ex) {
      return null;
    } catch (KeeperException ex) {
      throw new TokenStoreException(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreException(ex);
    }
  }

  @Override
  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() {
    String containerNode = rootNode + NODE_TOKENS;
    final List<String> nodes;
    try  {
      nodes = getSession().getChildren(containerNode, false);
    } catch (KeeperException ex) {
      throw new TokenStoreException(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreException(ex);
    }
    List<DelegationTokenIdentifier> result = new java.util.ArrayList<DelegationTokenIdentifier>(
        nodes.size());
    for (String node : nodes) {
      DelegationTokenIdentifier id = new DelegationTokenIdentifier();
      try {
        TokenStoreDelegationTokenSecretManager.decodeWritable(id, node);
        result.add(id);
      } catch (Exception e) {
        LOGGER.warn("Failed to decode token '{}'", node);
      }
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    if (this.zkSession != null) {
      try {
        this.zkSession.close();
      } catch (InterruptedException ex) {
        LOGGER.warn("Failed to close existing session.", ex);
      }
    }
  }

}
