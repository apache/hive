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

package org.apache.hive.minikdc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class TestZooKeeperWithMiniKdc {
  private static final String ZK_PRINCIPAL = "zookeeper";
  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniKDC;
  private static HiveConf conf;
  private static GenericContainer<?> zookeeper;

  @BeforeClass
  public static void setUp() throws Exception {
    miniKDC = new MiniHiveKdc();
    conf = new HiveConf();
    miniKDC.addUserPrincipal(miniKDC.getServicePrincipalForUser(ZK_PRINCIPAL));
    zookeeper = startZooKeeper(miniKDC, conf);
    String hiveMetastorePrincipal =
        miniKDC.getFullyQualifiedServicePrincipal(miniKDC.getHiveMetastoreServicePrincipal());
    String hiveMetastoreKeytab = miniKDC.getKeyTabFile(
        miniKDC.getServicePrincipalForUser(miniKDC.getHiveMetastoreServicePrincipal()));
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.USE_THRIFT_SASL, true);
    conf.set("hive.metastore.kerberos.principal", hiveMetastorePrincipal);
    conf.set("hive.metastore.kerberos.keytab.file", hiveMetastoreKeytab);
    conf.set("hive.cluster.delegation.token.store.class", ZooKeeperTokenStore.class.getName());
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.EXECUTE_SET_UGI, false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_SERVICE_DISCOVERY_MODE, "zookeeper");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_BIND_HOST, "localhost");

    DriverManager.setLoginTimeout(0);
    HiveConf.setVar(conf, HiveConf.ConfVars.METASTORE_URIS, "localhost:" + zookeeper.getMappedPort(2181));
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY, true);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS, true);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_FETCH_TASK_CACHING, false);
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerbWithRemoteHMSWithKerb(miniKDC, conf);
    miniHS2.start(new HashMap<String, String>());
  }

  @Test
  public void testMetaStoreClient() throws Exception {
    System.clearProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY);
    Assert.assertEquals("localhost:" + zookeeper.getMappedPort(2181),
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.THRIFT_URIS));
    try (HiveMetaStoreClient client = new HiveMetaStoreClient(conf)) {
      URI[] uris = client.getThriftClient().getMetastoreUris();
      Assert.assertEquals(1, uris.length);
      Assert.assertEquals(miniHS2.getHmsPort(), uris[0].getPort());
      client.addMasterKey("adbcedfghigklmn");
    }
    Assert.assertNotNull(System.getProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY));
  }

  @Test
  public void testJdbcConnection() throws Exception {
    System.clearProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY);
    String url = "jdbc:hive2://localhost:" + zookeeper.getMappedPort(2181) + "/default;" +
        "serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2;principal=hive/localhost@EXAMPLE.COM";
    try (Connection con = DriverManager.getConnection(url);
         ResultSet rs = con.getMetaData().getCatalogs()) {
      Assert.assertFalse(rs.next());
      ((HiveConnection) con).getDelegationToken("hive", "hive");
    }
    Assert.assertNotNull(System.getProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY));
  }

  static GenericContainer<?>
      startZooKeeper(MiniHiveKdc miniKDC, HiveConf conf) throws Exception {
    Pair<File, int[]> krb5Conf = forkNewKrbConf(miniKDC);
    GenericContainer<?> zookeeper = new GenericContainer<>(DockerImageName.parse("zookeeper:3.8.4"))
        .withExposedPorts(2181)
        .waitingFor(Wait.forLogMessage(".*binding to port.*2181.*\\n", 1))
        .withEnv("JVMFLAGS", "-Djava.security.auth.login.config=/conf/jaas.conf")
        .withEnv("ZOO_CFG_EXTRA", "authProvider.1=org.apache.zookeeper.server.auth.SASLAuthenticationProvider" +
            " sessionRequireClientSASLAuth=true")
        .withFileSystemBind(miniKDC.getKeyTabFile(miniKDC.getServicePrincipalForUser(ZK_PRINCIPAL)),
            "/conf/zookeeper.keytab", BindMode.READ_ONLY)
        .withFileSystemBind(TestZooKeeperWithMiniKdc.class.getClassLoader().getResource("zk_jaas.conf").getPath(),
            "/conf/jaas.conf", BindMode.READ_ONLY)
        .withFileSystemBind(krb5Conf.getLeft().getPath(), "/etc/krb5.conf", BindMode.READ_ONLY);
    if (krb5Conf.getRight().length > 0) {
      org.testcontainers.Testcontainers.exposeHostPorts(krb5Conf.getRight());
    }
    zookeeper.start();
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM, "localhost:" + zookeeper.getMappedPort(2181));
    return zookeeper;
  }

  private static Pair<File, int[]> forkNewKrbConf(MiniHiveKdc miniKDC) throws Exception {
    File krb5 = miniKDC.miniKdc.getKrb5conf();
    File newKrb5 = new File(miniKDC.workDir, krb5.getName() + "_new");
    List<Integer> hostPorts = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(krb5));
         FileWriter writer = new FileWriter(newKrb5, false)) {
      String line;
      String localhost = "localhost:";
      while ((line = reader.readLine()) != null) {
        if (line.contains(localhost)) {
          hostPorts.add(Integer.valueOf(line.split(localhost)[1]));
          line = line.replace("localhost", "host.testcontainers.internal");
        }
        writer.write(line);
        writer.write(System.lineSeparator());
      }
      writer.flush();
    }
    int[] ports = new int[hostPorts.size()];
    for (int i = 0; i < hostPorts.size(); i++) {
      ports[i] = hostPorts.get(i);
    }
    return Pair.of(newKrb5, ports);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
     if (zookeeper != null) {
       zookeeper.stop();
     }
    } finally {
      miniKDC.shutDown();
      if (miniHS2 != null && miniHS2.isStarted()) {
        miniHS2.stop();
        miniHS2.cleanup();
      }
    }
  }
}
