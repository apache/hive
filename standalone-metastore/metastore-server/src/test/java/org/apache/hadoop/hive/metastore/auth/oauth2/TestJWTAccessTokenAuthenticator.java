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

package org.apache.hadoop.hive.metastore.auth.oauth2;

import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.id.Issuer;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestJWTAccessTokenAuthenticator {
  private static final JOSEObjectType TYPE = new JOSEObjectType("at+jwt");
  private static final Issuer ISSUER = Issuer.parse("http://localhost:8080/auth");
  private static final String AUDIENCE = "http://localhost:8081/hive";
  private static final String USERNAME = "test-user";
  private static final List<String> SCOPES = List.of("read", "update");
  private static final Date FUTURE_DATE = new Date(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
  private static final JWTClaimsSet CLAIMS_SET = new JWTClaimsSet.Builder().issuer(ISSUER.getValue()).audience(AUDIENCE)
      .expirationTime(FUTURE_DATE).claim("email", USERNAME + "@example.com")
      .claim("scope", String.join(" ", SCOPES)).build();
  private static final Path JWT_AUTHKEY;
  private static final Path JWT_NOAUTHKEY;
  private static final URL JWKS_URL = TestJWTAccessTokenAuthenticator.class.getClassLoader()
      .getResource("auth/jwt/jwt-verification-jwks.json");
  private static final OAuth2PrincipalMapper PRINCIPAL_MAPPER = new RegexOAuth2PrincipalMapper("email",
      Pattern.compile("(.*)@example.com"));

  static {
    try {
      JWT_AUTHKEY = Path.of(Objects.requireNonNull(TestJWTAccessTokenAuthenticator.class.getClassLoader()
          .getResource("auth/jwt/jwt-authorized-key.json")).toURI());
      JWT_NOAUTHKEY = Path.of(Objects.requireNonNull(TestJWTAccessTokenAuthenticator.class.getClassLoader()
          .getResource("auth/jwt/jwt-unauthorized-key.json")).toURI());
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private static String generateJwt(JOSEObjectType type, JWTClaimsSet claimsSet, Path keyFile) {
    try {
      var rsaKeyPair = RSAKey.parse(Files.readString(keyFile));
      var signer = new RSASSASigner(rsaKeyPair);
      var header = new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(rsaKeyPair.getKeyID()).type(type).build();
      SignedJWT signedJWT = new SignedJWT(header, claimsSet);
      signedJWT.sign(signer);
      return signedJWT.serialize();
    } catch (Exception e) {
      throw new AssertionError("Unexpectedly failed to generate JWT", e);
    }
  }

  @Test
  public void testSuccess() throws HttpAuthenticationException {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var jwt = generateJwt(TYPE, CLAIMS_SET, JWT_AUTHKEY);
    var actual = authenticator.resolveUserName(jwt, SCOPES);
    Assert.assertEquals("test-user", actual);
  }

  @Test
  public void testExpired() {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var pastDate = new Date(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1));
    var claimSet = new JWTClaimsSet.Builder(CLAIMS_SET).expirationTime(pastDate).build();
    var jwt = generateJwt(TYPE, claimSet, JWT_AUTHKEY);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(jwt, SCOPES));
    Assert.assertEquals("Expired JWT", error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of(
        "Bearer error=\"invalid_token\", error_description=\"Invalid access token\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testNullToken() {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(null, SCOPES));
    Assert.assertEquals("Missing bearer token", error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of("Bearer"), error.getWwwAuthenticateHeader());
  }

  @Test
  public void testWrongJwtType() {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var jwt = generateJwt(JOSEObjectType.JWT, CLAIMS_SET, JWT_AUTHKEY);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(jwt, SCOPES));
    Assert.assertEquals("JOSE header typ (type) JWT not allowed", error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"invalid_token\", error_description=\"Invalid access token\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testWrongJson() {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName("invalid format", SCOPES));
    Assert.assertEquals("Invalid JWT serialization: Missing dot delimiter(s)", error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"invalid_token\", error_description=\"Invalid access token\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testWronglySignedJwt() {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var jwt = generateJwt(TYPE, CLAIMS_SET, JWT_NOAUTHKEY);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(jwt, SCOPES));
    Assert.assertEquals("Signed JWT rejected: Another algorithm expected, or no matching key(s) found",
        error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"invalid_token\", error_description=\"Invalid access token\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testWrongIssuer() {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var claimSet = new JWTClaimsSet.Builder(CLAIMS_SET).issuer("http://localhost:8080/wrong").build();
    var jwt = generateJwt(TYPE, claimSet, JWT_AUTHKEY);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(jwt, SCOPES));
    Assert.assertEquals("JWT iss claim has value http://localhost:8080/wrong, must be http://localhost:8080/auth",
        error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"invalid_token\", error_description=\"Invalid access token\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testWrongAudience() {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var claimSet = new JWTClaimsSet.Builder(CLAIMS_SET).audience("http://localhost:8080/wrong").build();
    var jwt = generateJwt(TYPE, claimSet, JWT_AUTHKEY);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(jwt, SCOPES));
    Assert.assertEquals("JWT audience rejected: [http://localhost:8080/wrong]",
        error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"invalid_token\", error_description=\"Invalid access token\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testMissingScope() {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var claimSet = new JWTClaimsSet.Builder(CLAIMS_SET).claim("scope", null).build();
    var jwt = generateJwt(TYPE, claimSet, JWT_AUTHKEY);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(jwt, SCOPES));
    Assert.assertEquals("This resource requires the following scopes: [read, update]",
        error.getMessage());
    Assert.assertEquals(403, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"insufficient_scope\", error_description=\"Insufficient scope\", scope=\"read update\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testInsufficientScope() {
    var authenticator = new JWTAccessTokenAuthenticator(ISSUER, JWKS_URL, AUDIENCE, PRINCIPAL_MAPPER);
    var claimSet = new JWTClaimsSet.Builder(CLAIMS_SET).claim("scope", "read delete").build();
    var jwt = generateJwt(TYPE, claimSet, JWT_AUTHKEY);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(jwt, SCOPES));
    Assert.assertEquals("Insufficient scopes: [update]", error.getMessage());
    Assert.assertEquals(403, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"insufficient_scope\", error_description=\"Insufficient scope\", scope=\"read update\""),
        error.getWwwAuthenticateHeader());
  }
}
