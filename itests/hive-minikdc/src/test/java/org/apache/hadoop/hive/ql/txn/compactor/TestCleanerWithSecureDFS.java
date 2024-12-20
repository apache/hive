/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.reflection.FieldSetter;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;

public class TestCleanerWithSecureDFS extends CompactorTest {
  private static final Path KEYSTORE_DIR =
      Paths.get(System.getProperty("test.tmp.dir"), "kdc_root_dir" + UUID.randomUUID());
  private static final String SUPER_USER_NAME = "hdfs";
  private static final Path SUPER_USER_KEYTAB = KEYSTORE_DIR.resolve(SUPER_USER_NAME + ".keytab");

  private static MiniDFSCluster dfsCluster = null;
  private static MiniKdc kdc = null;
  private static HiveConf secureConf = null;

  private static MiniKdc initKDC() {
    try {
      MiniKdc kdc = new MiniKdc(MiniKdc.createConf(), KEYSTORE_DIR.toFile());
      kdc.start();
      kdc.createPrincipal(SUPER_USER_KEYTAB.toFile(), SUPER_USER_NAME + "/localhost", "HTTP/localhost");
      return kdc;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static MiniDFSCluster initDFS(Configuration c) {
    try {
      MiniDFSCluster cluster = new MiniDFSCluster.Builder(c).numDataNodes(1).skipFsyncForTesting(true).build();
      cluster.waitActive();
      return cluster;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void startCluster() throws Exception {
    kdc = initKDC();
    secureConf = createSecureDFSConfig(kdc);
    dfsCluster = initDFS(secureConf);
  }

  @AfterClass
  public static void stopCluster() {
    secureConf = null;
    try {
      if (dfsCluster != null) {
        dfsCluster.close();
      }
    } finally {
      dfsCluster = null;
      if (kdc != null) {
        kdc.stop();
      }
      kdc = null;
    }
  }

  @Override
  public void setup() throws Exception {
    HiveConf conf = new HiveConf(secureConf);
    conf.set("fs.defaultFS", dfsCluster.getFileSystem().getUri().toString());
    setup(new HiveConf(secureConf));
  }

  private static HiveConf createSecureDFSConfig(MiniKdc kdc) throws Exception {
    HiveConf conf = new HiveConf();
    SecurityUtil.setAuthenticationMethod(UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    String suPrincipal = SUPER_USER_NAME + "/localhost@" + kdc.getRealm();
    String suKeyTab = SUPER_USER_KEYTAB.toAbsolutePath().toString();
    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, suPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, suKeyTab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, suPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, suKeyTab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, "HTTP/localhost@" + kdc.getRealm());
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication,integrity,privacy");
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.setInt(IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SASL_KEY, 10);
    conf.set("hadoop.proxyuser.hdfs.groups", "*");
    conf.set("hadoop.proxyuser.hdfs.hosts", "*");

    String sslConfDir = KeyStoreTestUtil.getClasspathDir(TestCleanerWithSecureDFS.class);
    KeyStoreTestUtil.setupSSLConfig(KEYSTORE_DIR.toAbsolutePath().toString(), sslConfDir, conf, false);
    return conf;
  }

  @Test
  public void testLeakAfterHistoryException() throws Exception {
    final String tblNamePrefix = "tbl_hive_26404_";
    // Generate compaction requests that will fail cause the base file will not meet the expectations
    for (int i = 0; i < 5; i++) {
      Table t = newTable("default", tblNamePrefix + i, false);
      CompactionRequest rqst = new CompactionRequest("default", t.getTableName(), CompactionType.MAJOR);
      // The requests should come from different users in order to trigger the leak
      rqst.setRunas("user" + i);
      long cTxn = compactInTxn(rqst);
      // We need at least a base compacted file in order to trigger the exception.
      // It must not be a valid base otherwise the cleaner will not throw.
      addBaseFile(t, null, 1, 1, cTxn);
    }
    Cleaner cleaner = new Cleaner();
    // Create a big configuration, (by adding lots of properties) to trigger the memory
    // leak fast. The problem can still be reproduced with small configurations but it requires more
    // compaction requests to fail and its not practical to have in a unit test.
    // The size of the configuration, measured by taking heapdump and inspecting the objects, is
    // roughly 190MB.
    HiveConf cleanerConf = new HiveConf(conf);
    for (int i = 0; i < 1_000_000; i++) {
      cleanerConf.set("hive.random.property.with.id." + i, Integer.toString(i));
    }
    cleaner.setConf(cleanerConf);
    cleaner.init(new AtomicBoolean(true));
    FieldSetter.setField(cleaner, MetaStoreCompactorThread.class.getDeclaredField("txnHandler"), txnHandler);
    Runtime.getRuntime().gc();
    long startMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    cleaner.run();
    Runtime.getRuntime().gc();
    long endMem = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    long diffMem = Math.abs(endMem - startMem);
    // 5 failed compactions X 190MB leak per config ~ 1GB leak
    // Depending on the Xmx value the leak may lead to OOM; if you definitely want to see the OOM
    // increase the size of the configuration or the number of failed compactions.
    Assert.assertTrue("Allocated memory, " + diffMem + "bytes , exceeds acceptable variance of 250MB.",
        diffMem < 250_000_000);
  }

  @Override
  boolean useHive130DeltaDirName() {
    return false;
  }
}
