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
import com.nimbusds.jose.KeyTypeException;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.apache.hadoop.hive.conf.HiveConf;

import javax.crypto.SecretKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Date;
import java.util.List;

public class JWTValidator {

  private final JWKSProvider jwksProvider;

  public JWTValidator(HiveConf conf) {
    // TODO - do we need to make provider configurable so that people can define their own ways of fetching JWKS ?
    this.jwksProvider = new URLBasedJWKSProvider(conf);
  }

  public String validateJWTAndExtractUser(String signedJwt) throws Exception {
    final SignedJWT parsedJwt = SignedJWT.parse(signedJwt);
    List<JWK> matchedJWKS = jwksProvider.getJWKs(parsedJwt.getHeader());

    // verify signature
    boolean signatureVerificationSuccessful = false;
    for (JWK matchedJWK : matchedJWKS) {
      JWSVerifier verifier = getVerifier(parsedJwt, matchedJWK);
      if (parsedJwt.verify(verifier)) {
        signatureVerificationSuccessful = true;
        break;
      }
    }
    Preconditions.checkState(signatureVerificationSuccessful, "Unable to verify incoming JWT Signature");

    // verify claims now
    JWTClaimsSet claimsSet = parsedJwt.getJWTClaimsSet();
    Date expirationTime = claimsSet.getExpirationTime();
    if (expirationTime != null) {
      Date now = new Date();
      Preconditions.checkState(now.before(expirationTime), "JWT has been expired");
    }

    // TODO We need to return query user from here - Subject will be query user ?
    return claimsSet.getSubject();
  }



  // TODO Same logic as DefaultJWSVerifierFactory#createJWSVerifier -- This can be written more elegantly, check AGAIN
  private static JWSVerifier getVerifier(JWSObject parsedJWT, JWK key) throws JOSEException {
    JWSVerifier verifier;
    JWSHeader header = parsedJWT.getHeader();
    if (MACVerifier.SUPPORTED_ALGORITHMS.contains(header.getAlgorithm())) {
      if (!(key instanceof SecretKey)) {
        throw new KeyTypeException(SecretKey.class);
      }
      SecretKey macKey = (SecretKey) key;
      verifier = new MACVerifier(macKey);
    } else if (RSASSAVerifier.SUPPORTED_ALGORITHMS.contains(header.getAlgorithm())) {
      // TODO currently only RSA branch is doing correct casting, need to correct others
      if (!(key instanceof RSAKey)) {
        throw new KeyTypeException(RSAPublicKey.class);
      }
      RSAPublicKey rsaPublicKey = ((RSAKey) key).toRSAPublicKey();
      verifier = new RSASSAVerifier(rsaPublicKey);
    } else if (ECDSAVerifier.SUPPORTED_ALGORITHMS.contains(header.getAlgorithm())) {
      if (!(key instanceof ECPublicKey)) {
        throw new KeyTypeException(ECPublicKey.class);
      }
      ECPublicKey ecPublicKey = (ECPublicKey) key;
      verifier = new ECDSAVerifier(ecPublicKey);
    } else {
      throw new JOSEException("Unsupported JWS algorithm: " + header.getAlgorithm());
    }
    return verifier;
  }

}
