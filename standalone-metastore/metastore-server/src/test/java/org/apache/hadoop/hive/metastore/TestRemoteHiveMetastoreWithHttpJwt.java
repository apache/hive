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

import com.github.stefanbirkner.systemlambda.SystemLambda;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.transport.TTransportException;

import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static org.junit.Assert.assertEquals;

@Category(MetastoreUnitTest.class)
public class TestRemoteHiveMetastoreWithHttpJwt {
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

  private static final int MOCK_JWKS_SERVER_PORT = 8089;
  @ClassRule
  public static final WireMockRule MOCK_JWKS_SERVER = new WireMockRule(MOCK_JWKS_SERVER_PORT);

  private static int port;
  private static Configuration conf = null;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
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
    LOG.info("Starting MetaStore Server on port {}", port);
  }

  private static void setupMockServer() throws Exception {
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
        .willReturn(ok()
            .withBody(Files.readAllBytes(jwtVerificationJWKSFile.toPath()))));
    MetastoreConf.setVar(conf, ConfVars.THRIFT_METASTORE_AUTHENTICATION_JWT_JWKS_URL,
        "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");
  }

  @Test
  public void testValidJWT() throws Exception {
    String validJwtToken = generateJWT(USER_1, jwtAuthorizedKeyFile.toPath(),
        TimeUnit.MINUTES.toMillis(5));

    SystemLambda.withEnvironmentVariable("HMS_JWT", validJwtToken).execute(() -> {
      String dbName = ("valid_jwt_" + TEST_DB_NAME_PREFIX + "_" + UUID.randomUUID()).toLowerCase();
      try (HiveMetaStoreClient client = new HiveMetaStoreClient(conf)) {
        Database createdDb = new Database();
        createdDb.setName(dbName);
        client.createDatabase(createdDb);
        Database dbFromServer = client.getDatabase(dbName);
        assertEquals(dbName, dbFromServer.getName());
        client.dropDatabase(dbName);
      }
    });
  }

  @Test(expected = TTransportException.class)
  public void testExpiredJWT() throws Exception {
    String validJwtToken = generateJWT(USER_1, jwtAuthorizedKeyFile.toPath(),
        TimeUnit.MILLISECONDS.toMillis(2));

    SystemLambda.withEnvironmentVariable("HMS_JWT", validJwtToken).execute(() -> {
      Thread.sleep(TimeUnit.MILLISECONDS.toMillis(3));
      try (HiveMetaStoreClient client = new HiveMetaStoreClient(conf)) {
        String dbName = ("expired_jwt_" + TEST_DB_NAME_PREFIX + "_" + UUID.randomUUID()).toLowerCase();
        Database createdDb = new Database();
        createdDb.setName(dbName);
        client.createDatabase(createdDb);
      }
    });
  }

  @Test(expected = TTransportException.class)
  public void testInvalidJWT() throws Exception {
    String jwtToken = generateJWT(USER_1, jwtUnauthorizedKeyFile.toPath(),
        TimeUnit.MINUTES.toMillis(2));

    SystemLambda.withEnvironmentVariable("HMS_JWT", jwtToken).execute(() -> {
      try (HiveMetaStoreClient client = new HiveMetaStoreClient(conf)) {
        String dbName = ("invalid_jwt_" + TEST_DB_NAME_PREFIX + "_" + UUID.randomUUID()).toLowerCase();
        Database createdDb = new Database();
        createdDb.setName(dbName);
        client.createDatabase(createdDb);
      }
    });
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
