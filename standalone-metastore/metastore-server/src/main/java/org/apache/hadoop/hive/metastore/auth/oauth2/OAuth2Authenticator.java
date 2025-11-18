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

import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.token.BearerTokenError;
import java.util.List;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;

/**
 * OAuth 2 based authenticator.
 */
public interface OAuth2Authenticator {

  /**
   * Resolves the username from the given bearer token.
   *
   * @param bearerToken the bearer access token in the "Authorization" HTTP header
   * @param requiredScopes the scopes required to access the resource
   * @return the username
   * @throws HttpAuthenticationException when it fails to resolve the bearer token
   */
  String resolveUserName(String bearerToken, List<String> requiredScopes) throws HttpAuthenticationException;

  static void requireBearerToken(String bearerToken) throws HttpAuthenticationException {
    if (bearerToken == null) {
      final var error = BearerTokenError.MISSING_TOKEN;
      throw new HttpAuthenticationException("Missing bearer token", error.getHTTPStatusCode(),
          error.toWWWAuthenticateHeader());
    }
  }

  static void requireScopes(Scope tokenScope, List<String> requiredScopes) throws HttpAuthenticationException {
    if (tokenScope == null) {
      final var error = BearerTokenError.INSUFFICIENT_SCOPE.setScope(Scope.parse(requiredScopes));
      throw new HttpAuthenticationException("This resource requires the following scopes: " + requiredScopes,
          error.getHTTPStatusCode(), error.toWWWAuthenticateHeader());
    }
    final var insufficient = requiredScopes.stream().filter(requiredScope -> !tokenScope.contains(requiredScope))
        .toList();
    if (!insufficient.isEmpty()) {
      final var error = BearerTokenError.INSUFFICIENT_SCOPE.setScope(Scope.parse(requiredScopes));
      throw new HttpAuthenticationException("Insufficient scopes: " + insufficient, error.getHTTPStatusCode(),
          error.toWWWAuthenticateHeader());
    }
  }
}
