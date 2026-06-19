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

package org.apache.hadoop.hive.metastore.auth.jwt;

import com.google.common.base.Preconditions;
import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSAlgorithm.Family;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.DefaultJOSEObjectTypeVerifier;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.net.URL;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

import java.util.List;

/**
 * This class is used to validate JWT. JWKS is fetched during instantiation and kept in the memory.
 * We disallow JWT signature verification with symmetric key, because that means anyone can get the same key
 * and use it to sign a JWT.
 * This is cloned from JWTValidator in HS2 so as to NOT have any dependency on HS2 code.
 */
public class JWTValidator {
  // Accept asymmetric cryptography based algorithms only
  private static final Set<JWSAlgorithm> ACCEPTABLE_ALGORITHMS = new HashSet<>(Family.SIGNATURE);

  private final ConfigurableJWTProcessor<SecurityContext> jwtProcessor;

  public JWTValidator(Set<JOSEObjectType> acceptableTypes, List<URL> jwksURLs, String expectedIssuer,
      String expectedAudience, Set<String> requiredClaimNames) {
    jwtProcessor = new DefaultJWTProcessor<>();
    jwtProcessor.setJWSTypeVerifier(new DefaultJOSEObjectTypeVerifier<>(acceptableTypes));
    Preconditions.checkArgument(!jwksURLs.isEmpty());
    final var keySelector = new JWSVerificationKeySelector<>(ACCEPTABLE_ALGORITHMS, getKeySource(jwksURLs));
    jwtProcessor.setJWSKeySelector(keySelector);
    final var expectedClaimsBuilder = new JWTClaimsSet.Builder();
    if (expectedIssuer != null) {
      expectedClaimsBuilder.issuer(expectedIssuer);
    }
    jwtProcessor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(expectedAudience, expectedClaimsBuilder.build(),
        requiredClaimNames));
  }

  private JWKSource<SecurityContext> getKeySource(List<URL> jwkURLs) {
    final var head = jwkURLs.getFirst();
    final var builder = JWKSourceBuilder.create(head).retrying(true);
    final var tail = jwkURLs.subList(1, jwkURLs.size());
    return tail.isEmpty() ? builder.build() : builder.failover(getKeySource(tail)).build();
  }

  public JWTClaimsSet validateJWT(String signedJwt) throws BadJOSEException, ParseException, JOSEException {
    Preconditions.checkNotNull(signedJwt, "No token found");
    return jwtProcessor.process(signedJwt, null);
  }
}
