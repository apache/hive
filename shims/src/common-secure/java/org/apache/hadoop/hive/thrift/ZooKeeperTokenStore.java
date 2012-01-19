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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.thrift.TokenStoreDelegationTokenSecretManager.TokenStoreError;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.HiveDelegationTokenSupport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooKeeper.States;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper token store implementation.
 */
public class ZooKeeperTokenStore implements TokenStoreDelegationTokenSecretManager.TokenStore {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(ZooKeeperTokenStore.class.getName());

  private static final String ZK_SEQ_FORMAT = "%010d";
  private static final String NODE_KEYS = "/keys";
  private static final String NODE_TOKENS = "/tokens";

  private String rootNode = "";
  private volatile ZooKeeper zkSession;
  private String zkConnectString;
  private final int zkSessionTimeout = 3000;

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
            zkSession = new ZooKeeper(this.zkConnectString, this.zkSessionTimeout,
                new ZooKeeperWatcher());
            } catch (IOException ex) {
              throw new TokenStoreError("Token store error.", ex);
            }
          }
        }
    }
    return zkSession;
  }

  private static String ensurePath(ZooKeeper zk, String path) throws KeeperException,
      InterruptedException {
    String[] pathComps = StringUtils.splitByWholeSeparator(path, "/");
    String currentPath = "";
    for (String pathComp : pathComps) {
      currentPath += "/" + pathComp;
      try {
        String node = zk.create(currentPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        LOGGER.info("Created path: " + node);
      } catch (KeeperException.NodeExistsException e) {
      }
    }
    return currentPath;
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
        ensurePath(zk, rootNode + NODE_KEYS);
        ensurePath(zk, rootNode + NODE_TOKENS);
      } catch (Exception e) {
        throw new TokenStoreError("Failed to validate token path.", e);
      }
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf != null) {
      this.zkConnectString = conf.get(
          HadoopThriftAuthBridge20S.Server.DELEGATION_TOKEN_STORE_ZK_CONNECT_STR, null);
      this.rootNode = conf.get(
          HadoopThriftAuthBridge20S.Server.DELEGATION_TOKEN_STORE_ZK_ROOT_NODE,
          HadoopThriftAuthBridge20S.Server.DELEGATION_TOKEN_STORE_ZK_ROOT_NODE_DEFAULT);
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
      String newNode = zk.create(rootNode + NODE_KEYS + "/", s.getBytes(), Ids.OPEN_ACL_UNSAFE,
          CreateMode.PERSISTENT_SEQUENTIAL);
      LOGGER.info("Added key {}", newNode);
      return getSeq(newNode);
    } catch (KeeperException ex) {
      throw new TokenStoreError(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreError(ex);
    }
  }

  @Override
  public void updateMasterKey(int keySeq, String s) {
    try {
      ZooKeeper zk = getSession();
      zk.setData(rootNode + NODE_KEYS + "/" + String.format(ZK_SEQ_FORMAT, keySeq), s.getBytes(),
          -1);
    } catch (KeeperException ex) {
      throw new TokenStoreError(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreError(ex);
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
      throw new TokenStoreError(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreError(ex);
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
      throw new TokenStoreError(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreError(ex);
    }
  }


  private String getTokenPath(DelegationTokenIdentifier tokenIdentifier) {
    try {
      return rootNode + NODE_TOKENS + "/"
          + TokenStoreDelegationTokenSecretManager.encodeWritable(tokenIdentifier);
    } catch (IOException ex) {
      throw new TokenStoreError("Failed to encode token identifier", ex);
    }
  }

  @Override
  public boolean addToken(DelegationTokenIdentifier tokenIdentifier,
      DelegationTokenInformation token) {
    try {
      ZooKeeper zk = getSession();
      byte[] tokenBytes = HiveDelegationTokenSupport.encodeDelegationTokenInformation(token);
      String newNode = zk.create(getTokenPath(tokenIdentifier),
          tokenBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      LOGGER.info("Added token: {}", newNode);
      return true;
    } catch (KeeperException.NodeExistsException ex) {
      return false;
    } catch (KeeperException ex) {
      throw new TokenStoreError(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreError(ex);
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
      throw new TokenStoreError(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreError(ex);
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
        throw new TokenStoreError("Failed to decode token", ex);
      }
    } catch (KeeperException.NoNodeException ex) {
      return null;
    } catch (KeeperException ex) {
      throw new TokenStoreError(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreError(ex);
    }
  }

  @Override
  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() {
    String containerNode = rootNode + NODE_TOKENS;
    final List<String> nodes;
    try  {
      nodes = getSession().getChildren(containerNode, false);
    } catch (KeeperException ex) {
      throw new TokenStoreError(ex);
    } catch (InterruptedException ex) {
      throw new TokenStoreError(ex);
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

}
