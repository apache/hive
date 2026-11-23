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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests TLS client authentication between the Metastore server and HiveMetaStoreClient, in binary and HTTP
 * transport modes.
 *
 * The test stores were generated with:
 * <pre>
 * keytool -genkeypair -alias server -keyalg RSA -validity 3650 -dname "CN=localhost, OU=Hive, O=Apache" \
 *   -ext "SAN=dns:localhost,ip:127.0.0.1" -storetype PKCS12 -keystore mtls_server_keystore.p12 \
 *   -storepass apache -keypass apache
 * keytool -genkeypair -alias client -keyalg RSA -validity 3650 -dname "CN=hive-client, OU=Hive, O=Apache" \
 *   -storetype PKCS12 -keystore mtls_client_keystore.p12 -storepass apache -keypass apache
 * keytool -genkeypair -alias untrusted -keyalg RSA -validity 3650 -dname "CN=untrusted-client, OU=Hive, O=Apache" \
 *   -storetype PKCS12 -keystore mtls_untrusted_client_keystore.p12 -storepass apache -keypass apache
 * keytool -exportcert -alias server -keystore mtls_server_keystore.p12 -storepass apache -file server.crt
 * keytool -exportcert -alias client -keystore mtls_client_keystore.p12 -storepass apache -file client.crt
 * keytool -importcert -noprompt -alias server -file server.crt -storetype PKCS12 \
 *   -keystore mtls_client_truststore.p12 -storepass apache
 * keytool -importcert -noprompt -alias client -file client.crt -storetype PKCS12 \
 *   -keystore mtls_server_truststore.p12 -storepass apache
 * </pre>
 */
@Category(MetastoreCheckinTest.class)
@RunWith(Parameterized.class)
public class TestMetastoreMutualTls {
  private static final String STORES_DIR = "src/test/resources";
  private static final String PASSWORD = "apache";
  private static final String SERVER_KEYSTORE = store("mtls_server_keystore.p12");
  private static final String SERVER_TRUSTSTORE = store("mtls_server_truststore.p12");
  private static final String CLIENT_KEYSTORE = store("mtls_client_keystore.p12");
  private static final String CLIENT_TRUSTSTORE = store("mtls_client_truststore.p12");
  private static final String UNTRUSTED_CLIENT_KEYSTORE = store("mtls_untrusted_client_keystore.p12");

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> transportModes() {
    return Arrays.asList(new Object[][] {{"binary"}, {"http"}});
  }

  private final String transportMode;
  private Configuration serverConf;
  private int port = -1;

  public TestMetastoreMutualTls(String transportMode) {
    this.transportMode = transportMode;
  }

  private static String store(String name) {
    return STORES_DIR + File.separator + name;
  }

  @After
  public void tearDown() {
    if (port > 0) {
      MetaStoreTestUtils.close(port);
      port = -1;
    }
  }

  private Configuration serverConf(String clientAuth) {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_TRANSPORT_MODE, transportMode);
    MetastoreConf.setBoolVar(conf, ConfVars.USE_SSL, true);
    MetastoreConf.setVar(conf, ConfVars.SSL_KEYSTORE_PATH, SERVER_KEYSTORE);
    MetastoreConf.setVar(conf, ConfVars.SSL_KEYSTORE_PASSWORD, PASSWORD);
    MetastoreConf.setVar(conf, ConfVars.SSL_KEYSTORE_TYPE, "PKCS12");
    MetastoreConf.setVar(conf, ConfVars.SSL_CLIENT_AUTH, clientAuth);
    return conf;
  }

  private void startServer(String clientAuth) throws Exception {
    serverConf = serverConf(clientAuth);
    MetastoreConf.setVar(serverConf, ConfVars.SSL_SERVER_TRUSTSTORE_PATH, SERVER_TRUSTSTORE);
    MetastoreConf.setVar(serverConf, ConfVars.SSL_SERVER_TRUSTSTORE_PASSWORD, PASSWORD);
    MetastoreConf.setVar(serverConf, ConfVars.SSL_SERVER_TRUSTSTORE_TYPE, "PKCS12");
    port = MetaStoreTestUtils.startMetaStoreWithRetry(serverConf);
  }

  private Configuration clientConf(String clientKeyStore) {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE, transportMode);
    MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 1);
    MetastoreConf.setBoolVar(conf, ConfVars.USE_SSL, true);
    MetastoreConf.setVar(conf, ConfVars.SSL_TRUSTSTORE_PATH, CLIENT_TRUSTSTORE);
    MetastoreConf.setVar(conf, ConfVars.SSL_TRUSTSTORE_PASSWORD, PASSWORD);
    MetastoreConf.setVar(conf, ConfVars.SSL_TRUSTSTORE_TYPE, "PKCS12");
    if (clientKeyStore != null) {
      MetastoreConf.setVar(conf, ConfVars.SSL_CLIENT_KEYSTORE_PATH, clientKeyStore);
      MetastoreConf.setVar(conf, ConfVars.SSL_CLIENT_KEYSTORE_PASSWORD, PASSWORD);
      MetastoreConf.setVar(conf, ConfVars.SSL_CLIENT_KEYSTORE_TYPE, "PKCS12");
    }
    return conf;
  }

  private void assertConnects(String clientKeyStore) throws Exception {
    try (HiveMetaStoreClient client = new HiveMetaStoreClient(clientConf(clientKeyStore))) {
      Assert.assertNotNull(client.getAllDatabases());
    }
  }

  private void assertRejected(String clientKeyStore) {
    try (HiveMetaStoreClient client = new HiveMetaStoreClient(clientConf(clientKeyStore))) {
      client.getAllDatabases();
      Assert.fail("Expected the TLS handshake to be rejected");
    } catch (Exception expected) {
      // handshake rejected by the server
    }
  }

  @Test
  public void testNoneAcceptsClientsWithAndWithoutCertificate() throws Exception {
    startServer("NONE");
    assertConnects(null);
    assertConnects(CLIENT_KEYSTORE);
  }

  @Test
  public void testWantAcceptsClientsWithoutCertificate() throws Exception {
    startServer("WANT");
    assertConnects(null);
    assertConnects(CLIENT_KEYSTORE);
    // The server only asks for certificates from the issuers in its truststore. The client has none from
    // those issuers, so it connects without sending a certificate, which WANT allows.
    assertConnects(UNTRUSTED_CLIENT_KEYSTORE);
  }

  @Test
  public void testNeedRequiresTrustedCertificate() throws Exception {
    startServer("need");
    assertConnects(CLIENT_KEYSTORE);
    assertRejected(null);
    assertRejected(UNTRUSTED_CLIENT_KEYSTORE);
  }

  @Test
  public void testClientAuthWithoutServerTrustStoreFails() throws Throwable {
    for (String clientAuth : new String[] {"WANT", "NEED"}) {
      Configuration conf = serverConf(clientAuth);
      // The metastore truststore is the client side truststore and is not used to verify client certificates.
      MetastoreConf.setVar(conf, ConfVars.SSL_TRUSTSTORE_PATH, SERVER_TRUSTSTORE);
      MetastoreConf.setVar(conf, ConfVars.SSL_TRUSTSTORE_PASSWORD, PASSWORD);
      int freePort = MetaStoreTestUtils.findFreePort();
      HadoopThriftAuthBridge bridge = HadoopThriftAuthBridge.getBridge();
      try {
        HiveMetaStore.startMetaStore(freePort, bridge, conf);
        Assert.fail("Expected " + clientAuth + " without a server truststore to fail at startup");
      } catch (IllegalArgumentException e) {
        Assert.assertEquals(ConfVars.SSL_SERVER_TRUSTSTORE_PATH + " Not configured for SSL client authentication",
            e.getMessage());
      }
    }
  }

  @Test
  public void testInvalidClientAuthFails() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.SSL_CLIENT_AUTH, "REQUIRED");
    try {
      MetastoreConf.getSslClientAuth(conf);
      Assert.fail("Expected an invalid client authentication mode to be rejected");
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Invalid value 'REQUIRED' for " + ConfVars.SSL_CLIENT_AUTH
          + ", expected one of [NONE, WANT, NEED]", e.getMessage());
    }
  }
}
