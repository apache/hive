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

package org.apache.hadoop.hive.metastore;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.transport.TTransportException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/*
Tests JWT auth in HiveMetastore server.
 */
@Category(MetastoreUnitTest.class)
public class TestRemoteHiveMetastoreWithHttpJwt {
  private static final Map<String, String> DEFAULTS = new HashMap<>(System.getenv());
  private static Map<String, String> envMap;

  private static String baseDir = System.getProperty("basedir");
  private static final File jwtAuthorizedKeyFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-authorized-key.json");
  private static final File jwtUnauthorizedKeyFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-unauthorized-key.json");
  private static final File jwtVerificationJWKSFile =
      new File(baseDir,"src/test/resources/auth/jwt/jwt-verification-jwks.json");

  private static final String USER_1 = "HMS_TEST_USER_1";
  private static final String TEST_DB_NAME_PREFIX = "HMS_JWT_AUTH_DB";
  private static final Logger LOG = LoggerFactory.getLogger(TestRemoteHiveMetastoreWithHttpJwt.class);
  //private static MiniHS2 miniHS2;

  private static final int MOCK_JWKS_SERVER_PORT = 8089;
  @ClassRule
  public static final WireMockRule MOCK_JWKS_SERVER = new WireMockRule(MOCK_JWKS_SERVER_PORT);

  /**
   * This is a hack to make environment variables modifiable.
   * Ref: https://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java.
   */
  @BeforeClass
  public static void makeEnvModifiable() throws Exception {
    envMap = new HashMap<>();
    Class<?> envClass = Class.forName("java.lang.ProcessEnvironment");
    Field theEnvironmentField = envClass.getDeclaredField("theEnvironment");
    Field theUnmodifiableEnvironmentField = envClass.getDeclaredField("theUnmodifiableEnvironment");
    removeStaticFinalAndSetValue(theEnvironmentField, envMap);
    removeStaticFinalAndSetValue(theUnmodifiableEnvironmentField, envMap);
  }

  private static void removeStaticFinalAndSetValue(Field field, Object value) throws Exception {
    field.setAccessible(true);
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    field.set(null, value);
  }
  private static int port;
  private static Configuration conf = null;

  public TestRemoteHiveMetastoreWithHttpJwt() {
    // default constructor
  }

  @AfterClass
  public static void stopServices() throws Exception {
    System.getenv().remove("HMS_JWT");
  }

  @BeforeClass
  public static void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();

    // set some values to use for getting conf. vars
    MetastoreConf.setBoolVar(conf, ConfVars.METRICS_ENABLED, true);
    conf.set("datanucleus.autoCreateTables", "false");
    conf.set("hive.in.test", "true");
    MetastoreConf.setVar(conf, ConfVars.METASTORE_METADATA_TRANSFORMER_CLASS, " ");

    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setLongVar(conf, ConfVars.BATCH_RETRIEVE_MAX, 2);
    MetastoreConf.setLongVar(conf, ConfVars.LIMIT_PARTITION_REQUEST, 100);
    MetastoreConf.setVar(conf, ConfVars.STORAGE_SCHEMA_READER_IMPL, "no.such.class");

    setupMockServer();
    MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_TRANSPORT_MODE, "http");
    MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_THRIFT_TRANSPORT_MODE, "http");

    MetastoreConf.setVar(conf, ConfVars.METASTORE_CLIENT_AUTH_MODE, "JWT");
    MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION, "JWT");
    startMetastoreServer();
  }

  private static void startMetastoreServer() throws Exception {
    port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(),
        conf);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    System.out.println("Starting MetaStore Server on port " + port);
  }

  @Before
  public void initEnvMap() {
    envMap.clear();
    envMap.putAll(DEFAULTS);
  }

  private static void setupMockServer() throws Exception {
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
        .willReturn(ok()
            .withBody(Files.readAllBytes(jwtVerificationJWKSFile.toPath()))));
    MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
        "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");
  }

  /*
  Tests a valid JWT sent to metastore sever
   */
  @Test
  public void testValidJWT() throws Exception {
    String validJwtToken = generateJWT(USER_1, jwtAuthorizedKeyFile.toPath(),
        TimeUnit.MINUTES.toMillis(5));
    System.getenv().put("HMS_JWT", validJwtToken);
    String dbName = ("valid_jwt_" + TEST_DB_NAME_PREFIX + "_" + UUID.randomUUID()).toLowerCase();
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    try {
      Database createdDb = new Database();
      createdDb.setName(dbName);
      client.createDatabase(createdDb);
      Database dbFromServer = client.getDatabase(dbName);
      assertEquals(dbName, dbFromServer.getName());
    } finally {
      try {
        client.dropDatabase(dbName);
      } catch (Exception e) {
        LOG.warn("Failed to drop database: " + dbName + ". Error message: " + e);
      }
      try {
        client.close();
      } catch (Exception e) {
        LOG.error("Failed to close metastore client");
      }
    }
  }

  /*
  Tests that an exception is thrown when metastore client (in http mode)
  sends a expired jwt to metastore server.
   */
  @Test(expected = TTransportException.class)
  public void testExpiredJWT() throws Exception {
    String validJwtToken = generateJWT(USER_1, jwtAuthorizedKeyFile.toPath(),
        TimeUnit.MILLISECONDS.toMillis(2));
    System.getenv().put("HMS_JWT", validJwtToken);
    String dbName = ("expired_jwt_" + TEST_DB_NAME_PREFIX + "_" + UUID.randomUUID()).toLowerCase();
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    try {
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(2));
      Database createdDb = new Database();
      createdDb.setName(dbName);
      client.createDatabase(createdDb);
    } catch (InterruptedException e) {
      // ignore
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        LOG.error("Failed to close metastore client");
      }
    }
  }

  /*
  Tests that an exception is thrown when metastore client (in http mode) sends an
  invalid jwt to the metastore server
   */
  @Test(expected = TTransportException.class)
  public void testInvalidJWT() throws Exception {
    String jwtToken = generateJWT(USER_1, jwtUnauthorizedKeyFile.toPath(),
        TimeUnit.MINUTES.toMillis(2));
    System.getenv().put("HMS_JWT", jwtToken);
    String dbName = ("invalid_jwt_" + TEST_DB_NAME_PREFIX + "_" + UUID.randomUUID()).toLowerCase();
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    try {
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(2));
      Database createdDb = new Database();
      createdDb.setName(dbName);
      client.createDatabase(createdDb);
    } catch (InterruptedException e) {
      // ignore
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        LOG.error("Failed to close metastore client");
      }
    }
  }

  private String generateJWT(String user, Path keyFile, long lifeTimeMillis) throws Exception {
    RSAKey rsaKeyPair = RSAKey.parse(new String(java.nio.file.Files.readAllBytes(keyFile),
        StandardCharsets.UTF_8));

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

}
