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

import com.google.common.base.Preconditions;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.factories.DefaultJWSVerifierFactory;
import com.nimbusds.jose.jwk.AsymmetricJWK;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.text.ParseException;
import java.util.Date;
import java.util.List;

/**
 * This class is used to validate JWT. JWKS is fetched during instantiation and kept in the memory.
 * We disallow JWT signature verification with symmetric key, because that means anyone can get the same key
 * and use it to sign a JWT.
 */
public class JWTValidator {

  private static final Logger LOG = LoggerFactory.getLogger(JWTValidator.class.getName());
  private static final DefaultJWSVerifierFactory JWS_VERIFIER_FACTORY = new DefaultJWSVerifierFactory();

  private final URLBasedJWKSProvider jwksProvider;

  public JWTValidator(HiveConf conf) throws IOException, ParseException, GeneralSecurityException {
    this.jwksProvider = new URLBasedJWKSProvider(conf);
  }

  public String validateJWTAndExtractUser(String signedJwt) throws ParseException, AuthenticationException {
    Preconditions.checkNotNull(jwksProvider);
    Preconditions.checkNotNull(signedJwt, "No token found");
    final SignedJWT parsedJwt = SignedJWT.parse(signedJwt);
    List<JWK> matchedJWKS = jwksProvider.getJWKs(parsedJwt.getHeader());
    if (matchedJWKS.isEmpty()) {
      throw new AuthenticationException("Failed to find matched JWKs with the JWT header: " + parsedJwt.getHeader());
    }

    // verify signature
    Exception lastException = null;
    for (JWK matchedJWK : matchedJWKS) {
      String keyID = matchedJWK.getKeyID() == null ? "null" : matchedJWK.getKeyID();
      try {
        JWSVerifier verifier = getVerifier(parsedJwt.getHeader(), matchedJWK);
        if (parsedJwt.verify(verifier)) {
          LOG.debug("Verified JWT {} by JWK {}", parsedJwt.getPayload(), keyID);
          break;
        }
      } catch (Exception e) {
        lastException = e;
        LOG.warn("Failed to verify JWT {} by JWK {}", parsedJwt.getPayload(), keyID, e);
      }
    }
    // We use only the last seven characters to let a user can differentiate exceptions for different JWT
    int startIndex = Math.max(0, signedJwt.length() - 7);
    String lastSevenChars = signedJwt.substring(startIndex);
    if (parsedJwt.getState() != JWSObject.State.VERIFIED) {
      throw new AuthenticationException("Failed to verify the JWT signature (ends with " + lastSevenChars + ")",
          lastException);
    }

    // verify claims
    JWTClaimsSet claimsSet = parsedJwt.getJWTClaimsSet();
    Date expirationTime = claimsSet.getExpirationTime();
    if (expirationTime != null) {
      Date now = new Date();
      if (now.after(expirationTime)) {
        LOG.warn("Rejecting an expired JWT: {}", parsedJwt.getPayload());
        throw new AuthenticationException("JWT (ends with " + lastSevenChars + ") has been expired");
      }
    }

    // We assume the subject of claims is the query user
    return claimsSet.getSubject();
  }

  private static JWSVerifier getVerifier(JWSHeader header, JWK jwk) throws JOSEException {
    Preconditions.checkArgument(jwk instanceof AsymmetricJWK,
        "JWT signature verification with symmetric key is not allowed.");
    Key key = ((AsymmetricJWK) jwk).toPublicKey();
    return JWS_VERIFIER_FACTORY.createJWSVerifier(header, key);
  }
}
