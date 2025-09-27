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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JOSEObjectType;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.oauth2.sdk.token.BearerTokenError;
import java.net.URL;
import java.text.ParseException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;
import org.apache.hadoop.hive.metastore.auth.jwt.JWTValidator;

/**
 * RFC 9068 compliant JWT access token authenticator.
 */
public class JWTAccessTokenAuthenticator implements OAuth2Authenticator {
  // RFC 9068 recommends using "at+jwt" to make sure the JWT is an access token, not an ID token or other tokens.
  // However, as RFC 9068 is relatively new, some providers still use "JWT" by default. So, this might be a little too
  // defensive.
  private static final Set<JOSEObjectType> ACCEPTABLE_TYPES = Collections.singleton(new JOSEObjectType("at+jwt"));

  private final JWTValidator validator;
  private final OAuth2PrincipalMapper principalMapper;

  public JWTAccessTokenAuthenticator(Issuer issuer, URL jwksURL, String audience,
      OAuth2PrincipalMapper principalMapper) {
    this.validator = new JWTValidator(ACCEPTABLE_TYPES, Collections.singletonList(jwksURL),
        issuer.getValue(), audience, Collections.emptySet());
    this.principalMapper = principalMapper;
  }

  @Override
  public String resolveUserName(String bearerToken, List<String> requiredScopes) throws HttpAuthenticationException {
    OAuth2Authenticator.requireBearerToken(bearerToken);
    final JWTClaimsSet claimSet;
    try {
      claimSet = validator.validateJWT(bearerToken);
    } catch (ParseException | BadJOSEException e) {
      final var error = BearerTokenError.INVALID_TOKEN;
      throw new HttpAuthenticationException(e.getMessage(), e, error.getHTTPStatusCode(),
          error.toWWWAuthenticateHeader());
    } catch (JOSEException e) {
      throw new HttpAuthenticationException("Unexpectedly failed to validate JWT", e, 500);
    }
    final Scope scope;
    try {
      scope = Scope.parse(claimSet.getStringClaim("scope"));
    } catch (ParseException e) {
      final var error = BearerTokenError.INVALID_TOKEN;
      throw new HttpAuthenticationException(e.getMessage(), e, error.getHTTPStatusCode(),
          error.toWWWAuthenticateHeader());
    }
    OAuth2Authenticator.requireScopes(scope, requiredScopes);
    return principalMapper.getUserName(claimName -> getStringFromClaim(claimSet, claimName));
  }

  private static String getStringFromClaim(JWTClaimsSet claimSet, String claimName) throws HttpAuthenticationException {
    try {
      return claimSet.getStringClaim(claimName);
    } catch (ParseException e) {
      final var error = BearerTokenError.INVALID_TOKEN;
      throw new HttpAuthenticationException(e.getMessage(), e, error.getHTTPStatusCode(),
          error.toWWWAuthenticateHeader());
    }
  }
}
