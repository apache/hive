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

package org.apache.hive.service.auth.jwt;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestHttpJwtAuthentication {
  private static final Map<String, String> DEFAULTS = new HashMap<>(System.getenv());
  private static Map<String, String> envMap;

  private static final File jwtAuthorizedKeyFile =
      new File("src/test/resources/auth.jwt/jwt-authorized-key.json");
  private static final File jwtUnauthorizedKeyFile =
      new File("src/test/resources/auth.jwt/jwt-unauthorized-key.json");
  private static final File jwtVerificationJWKSFile =
      new File("src/test/resources/auth.jwt/jwt-verification-jwks.json");

  public static final String USER_1 = "USER_1";

  private static MiniHS2 miniHS2;

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

  @Before
  public void initEnvMap() {
    envMap.clear();
    envMap.putAll(DEFAULTS);
  }

  @BeforeClass
  public static void setupHS2() throws Exception {
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
        .willReturn(ok()
            .withBody(Files.readAllBytes(jwtVerificationJWKSFile.toPath()))));

    HiveConf conf = new HiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);
    conf.setBoolVar(ConfVars.HIVESTATSCOLAUTOGATHER, false);
    conf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, "JWT");
    // the content of the URL below is the same as jwtVerificationJWKSFile
    conf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION_JWT_JWKS_URL, "http://localhost:" + MOCK_JWKS_SERVER_PORT +
        "/jwks");
    miniHS2 = new MiniHS2.Builder().withConf(conf).withHTTPTransport().build();

    miniHS2.start(new HashMap<>());
  }

  @AfterClass
  public static void stopServices() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
      miniHS2.cleanup();
      miniHS2 = null;
      MiniHS2.cleanupLocalDir();
    }
  }

  @Test
  public void testAuthorizedUser() throws Exception {
    String jwt = generateJWT(USER_1, jwtAuthorizedKeyFile.toPath(), TimeUnit.MINUTES.toMillis(5));
    HiveConnection connection = getConnection(jwt, true);
    assertLoggedInUser(connection, USER_1);
    connection.close();

    connection = getConnection(jwt, false);
    assertLoggedInUser(connection, USER_1);
    connection.close();
  }

  @Test(expected = SQLException.class)
  public void testExpiredJwt() throws Exception {
    String jwt = generateJWT(USER_1, jwtAuthorizedKeyFile.toPath(), 1);
    Thread.sleep(1);
    HiveConnection connection = getConnection(jwt, true);
  }

  @Test(expected = SQLException.class)
  public void testUnauthorizedUser() throws Exception {
    String unauthorizedJwt = generateJWT(USER_1, jwtUnauthorizedKeyFile.toPath(), TimeUnit.MINUTES.toMillis(5));
    HiveConnection connection = getConnection(unauthorizedJwt, true);
  }

  @Test(expected = SQLException.class)
  public void testWithoutJwtProvided() throws Exception {
    HiveConnection connection = getConnection(null, true);
  }

  private HiveConnection getConnection(String jwt, Boolean putJwtInEnv) throws Exception {
    String url = getJwtJdbcConnectionUrl();
    if (jwt != null && putJwtInEnv) {
      System.getenv().put(Utils.JdbcConnectionParams.AUTH_JWT_ENV, jwt);
    } else if (jwt != null) {
      url += "jwt=" + jwt;
    }
    Class.forName("org.apache.hive.jdbc.HiveDriver");
    Connection connection = DriverManager.getConnection(url, null, null);
    return (HiveConnection) connection;
  }

  private String generateJWT(String user, Path keyFile, long lifeTimeMillis) throws Exception {
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

  private String getJwtJdbcConnectionUrl() throws Exception {
    return miniHS2.getHttpJdbcURL() + "auth=jwt;";
  }

  private void assertLoggedInUser(HiveConnection connection, String expectedUser)
      throws SQLException {
    Statement stmt = connection.createStatement();
    ResultSet resultSet = stmt.executeQuery("select logged_in_user()");
    assertTrue(resultSet.next());
    String loggedInUser = resultSet.getString(1);
    assertEquals(expectedUser, loggedInUser);
  }
}
