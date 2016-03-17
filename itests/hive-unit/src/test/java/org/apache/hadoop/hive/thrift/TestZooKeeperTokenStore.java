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

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;
import org.apache.hadoop.security.token.delegation.HiveDelegationTokenSupport;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.junit.Assert;

public class TestZooKeeperTokenStore extends TestCase {

  private MiniZooKeeperCluster zkCluster = null;
  private CuratorFramework zkClient = null;
  private int zkPort = -1;
  private ZooKeeperTokenStore ts;

  @Override
  protected void setUp() throws Exception {
    File zkDataDir = new File(System.getProperty("test.tmp.dir"));
    if (this.zkCluster != null) {
      throw new IOException("Cluster already running");
    }
    this.zkCluster = new MiniZooKeeperCluster();
    this.zkPort = this.zkCluster.startup(zkDataDir);
    this.zkClient =
        CuratorFrameworkFactory.builder().connectString("localhost:" + zkPort)
            .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
    this.zkClient.start();
  }

  @Override
  protected void tearDown() throws Exception {
    this.zkClient.close();
    if (ts != null) {
      ts.close();
    }
    this.zkCluster.shutdown();
    this.zkCluster = null;
  }

  private Configuration createConf(String zkPath) {
    Configuration conf = new Configuration();
    conf.set(HiveDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_CONNECT_STR, "localhost:"
        + this.zkPort);
    conf.set(HiveDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_ZNODE, zkPath);
    return conf;
  }

  public void testTokenStorage() throws Exception {
    String ZK_PATH = "/zktokenstore-testTokenStorage";
    ts = new ZooKeeperTokenStore();
    Configuration conf = createConf(ZK_PATH);
    conf.set(HiveDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_ACL, "world:anyone:cdrwa");
    ts.setConf(conf);
    ts.init(null, ServerMode.METASTORE);


    String metastore_zk_path = ZK_PATH + ServerMode.METASTORE;
    int keySeq = ts.addMasterKey("key1Data");
    byte[] keyBytes = zkClient.getData().forPath(
        metastore_zk_path + "/keys/" + String.format(ZooKeeperTokenStore.ZK_SEQ_FORMAT, keySeq));
    assertNotNull(keyBytes);
    assertEquals(new String(keyBytes), "key1Data");

    int keySeq2 = ts.addMasterKey("key2Data");
    assertEquals("keys sequential", keySeq + 1, keySeq2);
    assertEquals("expected number keys", 2, ts.getMasterKeys().length);

    ts.removeMasterKey(keySeq);
    assertEquals("expected number keys", 1, ts.getMasterKeys().length);

    // tokens
    DelegationTokenIdentifier tokenId = new DelegationTokenIdentifier(
        new Text("owner"), new Text("renewer"), new Text("realUser"));
    DelegationTokenInformation tokenInfo = new DelegationTokenInformation(
        99, "password".getBytes());
    ts.addToken(tokenId, tokenInfo);
    DelegationTokenInformation tokenInfoRead = ts.getToken(tokenId);
    assertEquals(tokenInfo.getRenewDate(), tokenInfoRead.getRenewDate());
    assertNotSame(tokenInfo, tokenInfoRead);
    Assert.assertArrayEquals(HiveDelegationTokenSupport
        .encodeDelegationTokenInformation(tokenInfo),
        HiveDelegationTokenSupport
            .encodeDelegationTokenInformation(tokenInfoRead));

    List<DelegationTokenIdentifier> allIds = ts.getAllDelegationTokenIdentifiers();
    assertEquals(1, allIds.size());
    Assert.assertEquals(TokenStoreDelegationTokenSecretManager
        .encodeWritable(tokenId),
        TokenStoreDelegationTokenSecretManager.encodeWritable(allIds
            .get(0)));

    assertTrue(ts.removeToken(tokenId));
    assertEquals(0, ts.getAllDelegationTokenIdentifiers().size());
    assertNull(ts.getToken(tokenId));
  }

  public void testAclNoAuth() throws Exception {
    String ZK_PATH = "/zktokenstore-testAclNoAuth";
    Configuration conf = createConf(ZK_PATH);
    conf.set(
        HiveDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_ACL,
        "ip:127.0.0.1:r");

    ts = new ZooKeeperTokenStore();
    try {
      ts.setConf(conf);
      ts.init(null, ServerMode.METASTORE);
      fail("expected ACL exception");
    } catch (DelegationTokenStore.TokenStoreException e) {
      assertEquals(KeeperException.NoAuthException.class, e.getCause().getClass());
    }
  }

  public void testAclInvalid() throws Exception {
    String ZK_PATH = "/zktokenstore-testAclInvalid";
    String aclString = "sasl:hive/host@TEST.DOMAIN:cdrwa, fail-parse-ignored";
    Configuration conf = createConf(ZK_PATH);
    conf.set(
        HiveDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_ACL,
        aclString);

    List<ACL> aclList = ZooKeeperTokenStore.parseACLs(aclString);
    assertEquals(1, aclList.size());

    ts = new ZooKeeperTokenStore();
    try {
      ts.setConf(conf);
      ts.init(null, ServerMode.METASTORE);
      fail("expected ACL exception");
    } catch (DelegationTokenStore.TokenStoreException e) {
      assertEquals(KeeperException.InvalidACLException.class, e.getCause().getClass());
    }
  }

  public void testAclPositive() throws Exception {
    String ZK_PATH = "/zktokenstore-testAcl";
    Configuration conf = createConf(ZK_PATH);
    conf.set(
        HiveDelegationTokenManager.DELEGATION_TOKEN_STORE_ZK_ACL,
        "ip:127.0.0.1:cdrwa,world:anyone:cdrwa");
    ts = new ZooKeeperTokenStore();
    ts.setConf(conf);
    ts.init(null, ServerMode.METASTORE);
    List<ACL> acl = zkClient.getACL().forPath(ZK_PATH + ServerMode.METASTORE);
    assertEquals(2, acl.size());
  }

}
