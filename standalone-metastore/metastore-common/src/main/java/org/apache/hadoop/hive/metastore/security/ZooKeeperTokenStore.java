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

package org.apache.hadoop.hive.metastore.security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.MetastoreDelegationTokenSupport;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
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
  private volatile CuratorFramework zkSession;
  private String zkConnectString;
  private int connectTimeoutMillis;
  private List<ACL> newNodeAcl = Arrays.asList(new ACL(Perms.ALL, Ids.AUTH_IDS));

  /**
   * ACLProvider permissions will be used in case parent dirs need to be created
   */
  private final ACLProvider aclDefaultProvider =  new ACLProvider() {

    @Override
    public List<ACL> getDefaultAcl() {
      return newNodeAcl;
    }

    @Override
    public List<ACL> getAclForPath(String path) {
      return getDefaultAcl();
    }
  };


  private final String WHEN_ZK_DSTORE_MSG = "when zookeeper based delegation token storage is enabled"
      + "(hive.cluster.delegation.token.store.class=" + ZooKeeperTokenStore.class.getName() + ")";

  private Configuration conf;

  private HadoopThriftAuthBridge.Server.ServerMode serverMode;

  /**
   * Default constructor for dynamic instantiation w/ Configurable
   * (ReflectionUtils does not support Configuration constructor injection).
   */
  protected ZooKeeperTokenStore() {
  }

  private CuratorFramework getSession() {
    if (zkSession == null || zkSession.getState() == CuratorFrameworkState.STOPPED) {
      synchronized (this) {
        if (zkSession == null || zkSession.getState() == CuratorFrameworkState.STOPPED) {
          zkSession =
              CuratorFrameworkFactory.builder().connectString(zkConnectString)
                  .connectionTimeoutMs(connectTimeoutMillis).aclProvider(aclDefaultProvider)
                  .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
          zkSession.start();
        }
      }
    }
    return zkSession;
  }

  private void setupJAASConfig(Configuration conf) throws IOException {
    if (!UserGroupInformation.getLoginUser().isFromKeytab()) {
      // The process has not logged in using keytab
      // this should be a test mode, can't use keytab to authenticate
      // with zookeeper.
      LOGGER.warn("Login is not from keytab");
      return;
    }

    String principal;
    String keytab;
    switch (serverMode) {
    case METASTORE:
      principal = getNonEmptyConfVar(conf, "hive.metastore.kerberos.principal");
      keytab = getNonEmptyConfVar(conf, "hive.metastore.kerberos.keytab.file");
      break;
    case HIVESERVER2:
      principal = getNonEmptyConfVar(conf, "hive.server2.authentication.kerberos.principal");
      keytab = getNonEmptyConfVar(conf, "hive.server2.authentication.kerberos.keytab");
      break;
    default:
      throw new AssertionError("Unexpected server mode " + serverMode);
    }
    SecurityUtils.setZookeeperClientKerberosJaasConfig(principal, keytab);
  }

  private String getNonEmptyConfVar(Configuration conf, String param) throws IOException {
    String val = conf.get(param);
    if (val == null || val.trim().isEmpty()) {
      throw new IOException("Configuration parameter " + param + " should be set, "
          + WHEN_ZK_DSTORE_MSG);
    }
    return val;
  }

  /**
   * Create a path if it does not already exist ("mkdir -p")
   * @param path string with '/' separator
   * @param acl list of ACL entries
   * @throws TokenStoreException
   */
  public void ensurePath(String path, List<ACL> acl)
      throws TokenStoreException {
    try {
      CuratorFramework zk = getSession();
      String node = zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
          .withACL(acl).forPath(path);
      LOGGER.info("Created path: {} ", node);
    } catch (KeeperException.NodeExistsException e) {
      // node already exists
    } catch (Exception e) {
      throw new TokenStoreException("Error creating path " + path, e);
    }
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

  private void initClientAndPaths() {
    if (this.zkSession != null) {
      this.zkSession.close();
    }
    try {
      ensurePath(rootNode + NODE_KEYS, newNodeAcl);
      ensurePath(rootNode + NODE_TOKENS, newNodeAcl);
    } catch (TokenStoreException e) {
      throw e;
    }
  }

  @Override
  public void setConf(Configuration conf) {
    if (conf == null) {
      throw new IllegalArgumentException("conf is null");
    }
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return null; // not required
  }

  private Map<Integer, byte[]> getAllKeys() throws KeeperException, InterruptedException {

    String masterKeyNode = rootNode + NODE_KEYS;

    // get children of key node
    List<String> nodes = zkGetChildren(masterKeyNode);

    // read each child node, add to results
    Map<Integer, byte[]> result = new HashMap<Integer, byte[]>();
    for (String node : nodes) {
      String nodePath = masterKeyNode + "/" + node;
      byte[] data = zkGetData(nodePath);
      if (data != null) {
        result.put(getSeq(node), data);
      }
    }
    return result;
  }

  private List<String> zkGetChildren(String path) {
    CuratorFramework zk = getSession();
    try {
      return zk.getChildren().forPath(path);
    } catch (Exception e) {
      throw new TokenStoreException("Error getting children for " + path, e);
    }
  }

  private byte[] zkGetData(String nodePath) {
    CuratorFramework zk = getSession();
    try {
      return zk.getData().forPath(nodePath);
    } catch (KeeperException.NoNodeException ex) {
      return null;
    } catch (Exception e) {
      throw new TokenStoreException("Error reading " + nodePath, e);
    }
  }

  private int getSeq(String path) {
    String[] pathComps = path.split("/");
    return Integer.parseInt(pathComps[pathComps.length-1]);
  }

  @Override
  public int addMasterKey(String s) {
    String keysPath = rootNode + NODE_KEYS + "/";
    CuratorFramework zk = getSession();
    String newNode;
    try {
      newNode = zk.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).withACL(newNodeAcl)
          .forPath(keysPath, s.getBytes());
    } catch (Exception e) {
      throw new TokenStoreException("Error creating new node with path " + keysPath, e);
    }
    LOGGER.info("Added key {}", newNode);
    return getSeq(newNode);
  }

  @Override
  public void updateMasterKey(int keySeq, String s) {
    CuratorFramework zk = getSession();
    String keyPath = rootNode + NODE_KEYS + "/" + String.format(ZK_SEQ_FORMAT, keySeq);
    try {
      zk.setData().forPath(keyPath, s.getBytes());
    } catch (Exception e) {
      throw new TokenStoreException("Error setting data in " + keyPath, e);
    }
  }

  @Override
  public boolean removeMasterKey(int keySeq) {
    String keyPath = rootNode + NODE_KEYS + "/" + String.format(ZK_SEQ_FORMAT, keySeq);
    zkDelete(keyPath);
    return true;
  }

  private void zkDelete(String path) {
    CuratorFramework zk = getSession();
    try {
      zk.delete().forPath(path);
    } catch (KeeperException.NoNodeException ex) {
      // already deleted
    } catch (Exception e) {
      throw new TokenStoreException("Error deleting " + path, e);
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
    byte[] tokenBytes = MetastoreDelegationTokenSupport.encodeDelegationTokenInformation(token);
    String tokenPath = getTokenPath(tokenIdentifier);
    CuratorFramework zk = getSession();
    String newNode;
    try {
      newNode = zk.create().withMode(CreateMode.PERSISTENT).withACL(newNodeAcl)
          .forPath(tokenPath, tokenBytes);
    } catch (Exception e) {
      throw new TokenStoreException("Error creating new node with path " + tokenPath, e);
    }

    LOGGER.info("Added token: {}", newNode);
    return true;
  }

  @Override
  public boolean removeToken(DelegationTokenIdentifier tokenIdentifier) {
    String tokenPath = getTokenPath(tokenIdentifier);
    zkDelete(tokenPath);
    return true;
  }

  @Override
  public DelegationTokenInformation getToken(DelegationTokenIdentifier tokenIdentifier) {
    byte[] tokenBytes = zkGetData(getTokenPath(tokenIdentifier));
    if(tokenBytes == null) {
      // The token is already removed.
      return null;
    }
    try {
      return MetastoreDelegationTokenSupport.decodeDelegationTokenInformation(tokenBytes);
    } catch (Exception ex) {
      throw new TokenStoreException("Failed to decode token", ex);
    }
  }

  @Override
  public List<DelegationTokenIdentifier> getAllDelegationTokenIdentifiers() {
    String containerNode = rootNode + NODE_TOKENS;
    final List<String> nodes = zkGetChildren(containerNode);
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
      this.zkSession.close();
    }
  }

  @Override
  public void init(Object hmsHandler, HadoopThriftAuthBridge.Server.ServerMode sMode) {
    this.serverMode = sMode;
    zkConnectString =
        conf.get(MetastoreDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_CONNECT_STR, null);
    if (zkConnectString == null || zkConnectString.trim().isEmpty()) {
      // try alternate config param
      zkConnectString =
          conf.get(
              MetastoreDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_CONNECT_STR_ALTERNATE,
              null);
      if (zkConnectString == null || zkConnectString.trim().isEmpty()) {
        throw new IllegalArgumentException("Zookeeper connect string has to be specified through "
            + "either " + MetastoreDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_CONNECT_STR
            + " or "
            + MetastoreDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_CONNECT_STR_ALTERNATE
            + WHEN_ZK_DSTORE_MSG);
      }
    }
    connectTimeoutMillis =
        conf.getInt(
            MetastoreDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_CONNECT_TIMEOUTMILLIS,
            CuratorFrameworkFactory.builder().getConnectionTimeoutMs());
    String aclStr = conf.get(MetastoreDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_ACL, null);
    if (StringUtils.isNotBlank(aclStr)) {
      this.newNodeAcl = parseACLs(aclStr);
    }
    rootNode =
        conf.get(MetastoreDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_ZNODE,
            MetastoreDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_ZNODE_DEFAULT) + serverMode;

    try {
      // Install the JAAS Configuration for the runtime
      setupJAASConfig(conf);
    } catch (IOException e) {
      throw new TokenStoreException("Error setting up JAAS configuration for zookeeper client "
          + e.getMessage(), e);
    }
    initClientAndPaths();
  }

}
