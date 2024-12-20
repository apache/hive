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

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveServer2TransportMode;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;

/**
 * Test for supporting Kerberos + JWT in HS2 http mode
 *
 */
public class TestHS2JWTWithMiniKdc {
  private static final int MOCK_JWKS_SERVER_PORT = 8089;
  private static final File jwtAuthorizedKeyFile =
      new File("src/test/resources/auth.jwt/jwt-authorized-key.json");
  private static final File jwtUnauthorizedKeyFile =
      new File("src/test/resources/auth.jwt/jwt-unauthorized-key.json");
  private static final File jwtVerificationJWKSFile =
      new File("src/test/resources/auth.jwt/jwt-verification-jwks.json");

  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;

  @ClassRule
  public static final WireMockRule MOCK_JWKS_SERVER = new WireMockRule(MOCK_JWKS_SERVER_PORT);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
        .willReturn(ok()
            .withBody(Files.readAllBytes(jwtVerificationJWKSFile.toPath()))));
    miniHiveKdc = new MiniHiveKdc();
    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, HiveServer2TransportMode.http.name());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION_JWT_JWKS_URL, "http://localhost:" +
        MOCK_JWKS_SERVER_PORT + "/jwks");

    miniHS2 = MiniHiveKdc.getMiniHS2WithKerb(miniHiveKdc, hiveConf,
        HiveAuthConstants.AuthTypes.KERBEROS.getAuthName() + "," + HiveAuthConstants.AuthTypes.JWT.getAuthName());
    miniHS2.start(new HashMap<>());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    miniHS2.stop();
  }

  @Test
  public void testKrbJwtAuth() throws Exception {
    String krb5Url = new StringBuilder(miniHS2.getBaseHttpJdbcURL())
        .append("default;")
        .append("transportMode=http;httpPath=cliservice;")
        .append("principal=" + miniHiveKdc.getFullHiveServicePrincipal())
        .toString();

    try (Connection hs2Conn = DriverManager.getConnection(krb5Url)) {
      try (Statement statement = hs2Conn.createStatement()) {
        statement.execute("create table if not exists test_hs2_with_jwt_kerberos(a string)");
        statement.execute("set hive.support.concurrency");
        validateResult(statement.getResultSet(), 1);
      }
    }

    StringBuilder jwtUrl = new StringBuilder(miniHS2.getBaseHttpJdbcURL())
        .append("default;")
        .append("transportMode=http;httpPath=cliservice;")
        .append("auth=jwt;")
        .append("jwt=");

    try (Connection hs2Conn = DriverManager.getConnection(jwtUrl
        .append(generateJWT("user1", true, TimeUnit.MINUTES.toMillis(5)))
        .toString())) {
      try (Statement statement = hs2Conn.createStatement()) {
        statement.execute("create table if not exists test_hs2_with_jwt_kerberos(a string)");
        statement.execute("set hive.support.concurrency");
        validateResult(statement.getResultSet(), 1);
      }
    }

    try {
      DriverManager.getConnection(jwtUrl
          .append(generateJWT("user1", false, TimeUnit.MINUTES.toMillis(5)))
          .toString());
      Assert.fail("Exception is expected as JWT token is invalid");
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("HTTP Response code: 401"));
    }
  }

  private String generateJWT(String user, boolean authorized, long lifeTimeMillis) throws Exception {
    final byte[] content = authorized ?
        Files.readAllBytes(jwtAuthorizedKeyFile.toPath()) :
        Files.readAllBytes(jwtUnauthorizedKeyFile.toPath());
    RSAKey rsaKeyPair = RSAKey.parse(new String(content, StandardCharsets.UTF_8));

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

  private void validateResult(ResultSet rs, int expectedSize) throws Exception {
    int actualSize = 0;
    while (rs.next()) {
      actualSize ++;
    }
    Assert.assertEquals(expectedSize, actualSize);
  }

}
