/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest.extension;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class JwksServer {
  private static final String BASE_DIR = System.getProperty("basedir");
  private static final File JWT_AUTHKEY_FILE =
      new File(BASE_DIR, "src/test/resources/auth/jwt/jwt-authorized-key.json");
  private static final File JWT_NOAUTHKEY_FILE =
      new File(BASE_DIR, "src/test/resources/auth/jwt/jwt-unauthorized-key.json");
  private static final File JWT_JWKS_FILE = new File(BASE_DIR, "src/test/resources/auth/jwt/jwt-verification-jwks.json");
  private static final int MOCK_JWKS_SERVER_PORT = 8089;

  private WireMockRule mockJwksSever;

  void start() throws IOException {
    mockJwksSever = new WireMockRule(MOCK_JWKS_SERVER_PORT);
    byte[] body = Files.readAllBytes(JWT_JWKS_FILE.toPath());
    mockJwksSever.stubFor(WireMock.get("/jwks").willReturn(WireMock.ok().withBody(body)));
    mockJwksSever.start();
  }

  void stop() {
    mockJwksSever.stop();
  }

  int getPort() {
    return MOCK_JWKS_SERVER_PORT;
  }

  public static String generateValidJWT(String user) throws Exception {
    return generateJWT(user, JWT_AUTHKEY_FILE.toPath());
  }

  public static String generateInvalidJWT(String user) throws Exception {
    return generateJWT(user, JWT_NOAUTHKEY_FILE.toPath());
  }

  private static String generateJWT(String user, Path keyFile) throws Exception {
    RSAKey rsaKeyPair = RSAKey.parse(Files.readString(keyFile));
    // Create RSA-signer with the private key
    JWSSigner signer = new RSASSASigner(rsaKeyPair);
    JWSHeader header = new JWSHeader
        .Builder(JWSAlgorithm.RS256)
        .keyID(rsaKeyPair.getKeyID())
        .build();
    Date now = new Date();
    Date expirationTime = new Date(now.getTime() + TimeUnit.MINUTES.toMillis(5));
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
