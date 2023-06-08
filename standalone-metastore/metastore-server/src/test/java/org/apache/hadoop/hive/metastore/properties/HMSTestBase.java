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
package org.apache.hadoop.hive.metastore.properties;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.commons.io.IOUtils;
import org.apache.commons.jexl3.JxltEngine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.TestObjectStore;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MaintenanceOpStatus;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MaintenanceOpType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.JEXL;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MAINTENANCE_OPERATION;
import static org.apache.hadoop.hive.metastore.properties.HMSPropertyManager.MAINTENANCE_STATUS;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.BOOLEAN;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DATETIME;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.DOUBLE;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.INTEGER;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.JSON;
import static org.apache.hadoop.hive.metastore.properties.PropertyType.STRING;

public abstract class HMSTestBase {
  protected static final String baseDir = System.getProperty("basedir");
  private static final File jwtAuthorizedKeyFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-authorized-key.json");
  protected static final File jwtUnauthorizedKeyFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-unauthorized-key.json");
  protected static final File jwtVerificationJWKSFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-verification-jwks.json");

  public static final String USER_1 = "USER_1";

  protected static final int MOCK_JWKS_SERVER_PORT = 8089;
  @ClassRule
  public static final WireMockRule MOCK_JWKS_SERVER = new WireMockRule(MOCK_JWKS_SERVER_PORT);
  // the url part
  /**
   * Abstract the property client access on a given namespace.
   */
  interface PropertyClient {
    boolean setProperties(Map<String, String> properties);
    Map<String, Map<String, String>> getProperties(String mapPrefix, String mapPredicate, String... selection) throws IOException;
  }

  interface HttpPropertyClient extends PropertyClient {
    default Map<String, String> getProperties(List<String> selection) throws IOException {
      throw new UnsupportedOperationException("not implemented in " + this.getClass());
    }
  }

  protected Configuration conf = null;

  protected static final Logger LOG = LoggerFactory.getLogger(TestObjectStore.class.getName());
  static Random RND = new Random(20230424);
  protected String NS;// = "hms" + RND.nextInt(100);
  protected PropertyClient client;
  protected int port = -1;

  @Before
  public void setUp() throws Exception {
    NS = "hms" + RND.nextInt(100);
    conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);

    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    // Events that get cleaned happen in batches of 1 to exercise batching code
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.EVENT_CLEAN_MAX_EVENTS, 1L);
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_PORT, 0);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PROPERTIES_SERVLET_AUTH, "JWT");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
        "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
        .willReturn(ok()
            .withBody(Files.readAllBytes(jwtVerificationJWKSFile.toPath()))));
    // The server
    port = createServer(conf);
    System.out.println("Starting MetaStore Server on port " + port);
    // The manager decl
    PropertyManager.declare(NS, HMSPropertyManager.class);
    // The client
    client = createClient(conf, port);
    Assert.assertNotNull("Unable to connect to the MetaStore server", client);
  }

  @After
  public synchronized void tearDown() throws Exception {
    try {
      if (port >= 0) {
        stopServer(port);
        port = -1;
      }
      // Clear the SSL system properties before each test.
      System.clearProperty(ObjectStore.TRUSTSTORE_PATH_KEY);
      System.clearProperty(ObjectStore.TRUSTSTORE_PASSWORD_KEY);
      System.clearProperty(ObjectStore.TRUSTSTORE_TYPE_KEY);
      //
    } finally {
      client = null;
      conf = null;
    }
  }

  protected String generateJWT()  throws Exception {
    return generateJWT(jwtAuthorizedKeyFile.toPath());
  }
  protected String generateJWT(Path path)  throws Exception {
    return generateJWT(USER_1, path, TimeUnit.MINUTES.toMillis(5));
  }

  private static String generateJWT(String user, Path keyFile, long lifeTimeMillis) throws Exception {
    RSAKey rsaKeyPair = RSAKey.parse(new String(java.nio.file.Files.readAllBytes(keyFile), StandardCharsets.UTF_8));
    // Create RSA-signer with the private key
    JWSSigner signer = new RSASSASigner(rsaKeyPair);
    JWSHeader header = new JWSHeader
        .Builder(JWSAlgorithm.RS256)
        .keyID(rsaKeyPair.getKeyID())
        .build();
    Date now = new Date();
    Date expirationTime = new Date(now.getTime() + lifeTimeMillis);
    JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
        .jwtID(UUID.randomUUID().toString())
        .issueTime(now)
        .issuer("auth-server")
        .subject(user)
        .expirationTime(expirationTime)
        .claim("custom-claim-or-payload", "custom-claim-or-payload")
        .build();
    SignedJWT signedJWT = new SignedJWT(header, claimsSet);
    // Compute the RSA signature
    signedJWT.sign(signer);
    return signedJWT.serialize();
  }


  /**
   * Creates and starts the server.
   * @param conf
   * @return the server port
   * @throws Exception
   */
  protected int createServer(Configuration conf) throws Exception {
    return 0;
  }

  /**
   * Stops the server.
   * @param port the server port
   * @throws Exception
   */
  protected void stopServer(int port) throws Exception {
    // nothing
  }

  /**
   * Creates a client.
   * @return the client instance
   * @throws Exception
   */
  protected abstract PropertyClient createClient(Configuration conf, int port) throws Exception;


  public void runOtherProperties0(PropertyClient client) throws Exception {
    Map<String, String> ptyMap = createProperties0();
    boolean commit = client.setProperties(ptyMap);
    Assert.assertTrue(commit);
    // select tables whose policy table name starts with table0
    Map<String, Map<String, String>> maps = client.getProperties("db0.table", "policy.'Table-name' =^ 'table0'");
    Assert.assertNotNull(maps);
    Assert.assertEquals(8, maps.size());

    // select
    Map<String, Map<String, String>> project = client.getProperties("db0.tabl", "fillFactor > 92",
        "fillFactor",
        "{ 'policy' : { 'Compaction' : { 'target-size' : policy.Compaction.'target-size' } } }");
    Assert.assertNotNull(project);
    Assert.assertEquals(2, project.size());
  }

  static Map<String, String> createProperties0() {
    // configure hms
    HMSPropertyManager.declareTableProperty("fillFactor", DOUBLE, 0.75d);
    HMSPropertyManager.declareTableProperty("policy", JSON, null);
    HMSPropertyManager.declareTableProperty("maint_status", MAINTENANCE_STATUS, null);
    HMSPropertyManager.declareTableProperty("maint_operation", MAINTENANCE_OPERATION, null);
    // link store and manager
    try {
      String json = IOUtils.toString(
          HMSDirectTest.class.getResourceAsStream("payload.json"),
          "UTF-8"
      );
      JxltEngine JXLT = JEXL.createJxltEngine();
      JxltEngine.Template jsonjexl = JXLT.createTemplate(json, "table", "delta", "g");
      Assert.assertNotNull(json);
      Map<String, String> ptyMap = new TreeMap<>();
      for (int i = 0; i < 16; ++i) {
        String tname = "table" + String.format("%1$02o", i);
        String tb = "db0." + tname + ".";
        ptyMap.put(tb + "fillFactor", Integer.toString(100 - (5 * i)));

        StringWriter strw = new StringWriter();
        jsonjexl.evaluate(null, strw, tname, i * i % 100, (i + 1) % 7);
        ptyMap.put(tb + "policy", strw.toString());

        HMSPropertyManager.MaintenanceOpStatus status = HMSPropertyManager.findOpStatus( i % MaintenanceOpStatus.values().length);
        Assert.assertNotNull(status);
        ptyMap.put(tb + "maint_status", status.toString());

        HMSPropertyManager.MaintenanceOpType type = HMSPropertyManager.findOpType(i % MaintenanceOpType.values().length);
        Assert.assertNotNull(type);
        ptyMap.put(tb + "maint_operation", type.toString());
      }
      return ptyMap;
    } catch (IOException xio) {
      return null;
    }
  }

  public void runOtherProperties1(PropertyClient client) throws Exception {
    Map<String, String> ptyMap = createProperties1();
    boolean commit = client.setProperties(ptyMap);
    Assert.assertTrue(commit);
    // go get some
    Map<String, Map<String, String>> maps = client.getProperties("db1.table", "someb && fillFactor < 95");
    Assert.assertNotNull(maps);
    Assert.assertEquals(5, maps.size());

    if (client instanceof HttpPropertyClient) {
      HttpPropertyClient httpClient = (HttpPropertyClient) client;
      // get fillfactors using getProperties, create args array from previous result
      List<String> keys = new ArrayList<>(maps.keySet());
      for (int k = 0; k < keys.size(); ++k) {
        keys.set(k, keys.get(k) + ".fillFactor");
      }
      Object values = httpClient.getProperties(keys);
      Assert.assertTrue(values instanceof Map);
      Map<String, String> getm = (Map<String, String>) values;
      for (Map.Entry<String, Map<String, String>> entry : maps.entrySet()) {
        Map<String, String> map0v = entry.getValue();
        Assert.assertEquals(map0v.get("fillFactor"), getm.get(entry.getKey() + ".fillFactor"));
      }
    }

    maps = client.getProperties("db1.table", "fillFactor > 92", "fillFactor");
    Assert.assertNotNull(maps);
    Assert.assertEquals(8, maps.size());
  }

  static Map<String, String> createProperties1() {
    // configure hms
    HMSPropertyManager.declareTableProperty("id", INTEGER, null);
    HMSPropertyManager.declareTableProperty("name", STRING, null);
    HMSPropertyManager.declareTableProperty("uuid", STRING, null);
    HMSPropertyManager.declareTableProperty("fillFactor", DOUBLE, 0.75d);
    HMSPropertyManager.declareTableProperty("someb", BOOLEAN, Boolean.FALSE);
    HMSPropertyManager.declareTableProperty("creation date", DATETIME, "2023-01-06T12:16:00");
    HMSPropertyManager.declareTableProperty("project", STRING, "Hive");
    HMSPropertyManager.declareTableProperty("policy", JSON, null);
    HMSPropertyManager.declareTableProperty("maint_status", MAINTENANCE_STATUS, null);
    HMSPropertyManager.declareTableProperty("maint_operation", MAINTENANCE_OPERATION, null);
    // use properties to init
    Map<String, String> ptys = new TreeMap<>();
    for (int i = 0; i < 16; ++i) {
      String tb = "db1.table" + String.format("%1$02o", i) + ".";
      ptys.put(tb + "id", Integer.toString(1000 + i));
      ptys.put(tb + "name", "TABLE_" + i);
      ptys.put(tb + "fillFactor", Integer.toString(100 - i));
      ptys.put(tb + "someb", (i % 2) == 0 ? "true" : "false");
    }
    return ptys;
  }

}
