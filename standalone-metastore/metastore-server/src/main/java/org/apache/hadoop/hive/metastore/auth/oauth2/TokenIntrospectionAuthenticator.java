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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.TokenIntrospectionRequest;
import com.nimbusds.oauth2.sdk.TokenIntrospectionResponse;
import com.nimbusds.oauth2.sdk.TokenIntrospectionSuccessResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.BearerTokenError;
import com.nimbusds.oauth2.sdk.token.TokenSchemeError;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import javax.servlet.http.HttpServletResponse;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * RFC 7662 compliant OAuth2 token introspection based authenticator. This is must when access tokens are opaque, or
 * this is helpful when you need further security requirements (e.g., to check token revocation).
 */
public class TokenIntrospectionAuthenticator implements OAuth2Authenticator {
  private static final class UncheckedException extends RuntimeException {
    private final HttpAuthenticationException underlying;

    private UncheckedException(HttpAuthenticationException cause) {
      this.underlying = cause;
    }
  }

  private record TokenExpiry(Duration maxExpiration, Clock clock) implements
      Expiry<String, TokenIntrospectionSuccessResponse> {
    @Override
    public long expireAfterCreate(@NonNull String key, @NonNull TokenIntrospectionSuccessResponse value,
        long currentTime) {
      final var expiresIn = Duration.between(value.getExpirationTime().toInstant(), clock.instant());
      return Math.min(expiresIn.toNanos(), maxExpiration.toNanos());
    }

    @Override
    public long expireAfterUpdate(@NonNull String key, @NonNull TokenIntrospectionSuccessResponse value,
        long currentTime, @NonNegative long currentDuration) {
      final var expiresIn = Duration.between(value.getExpirationTime().toInstant(), clock.instant());
      return Math.min(expiresIn.toNanos(), maxExpiration.toNanos());
    }

    @Override
    public long expireAfterRead(@NonNull String key, @NonNull TokenIntrospectionSuccessResponse value, long currentTime,
        @NonNegative long currentDuration) {
      return currentDuration;
    }
  }

  private final URI introspectionEndpoint;
  private final Audience audience;
  private final ClientAuthentication credential;
  private final OAuth2PrincipalMapper principalMapper;
  private final Cache<String, TokenIntrospectionSuccessResponse> cache;

  public TokenIntrospectionAuthenticator(URI introspectionEndpoint, Audience audience, ClientAuthentication credential,
      OAuth2PrincipalMapper principalMapper, Duration maxCacheDuration, long cacheSize) {
    this.introspectionEndpoint = introspectionEndpoint;
    this.audience = audience;
    this.credential = credential;
    this.principalMapper = principalMapper;
    this.cache = maxCacheDuration.isPositive()
        ? Caffeine.newBuilder().maximumSize(cacheSize)
          .expireAfter(new TokenExpiry(maxCacheDuration, Clock.systemUTC())).build()
        : null;
  }

  @Override
  public String resolveUserName(String bearerToken, List<String> requiredScopes) throws HttpAuthenticationException {
    OAuth2Authenticator.requireBearerToken(bearerToken);
    final var result = cache == null ? postIntrospection(bearerToken) : postIntrospectionWithCache(bearerToken);
    OAuth2Authenticator.requireScopes(result.getScope(), requiredScopes);
    return principalMapper.getUserName(result::getStringParameter);
  }

  private TokenIntrospectionSuccessResponse postIntrospectionWithCache(String bearerToken)
      throws HttpAuthenticationException {
    try {
      return cache.get(bearerToken, token -> {
        try {
          return postIntrospection(bearerToken);
        } catch (HttpAuthenticationException e) {
          throw new UncheckedException(e);
        }
      });
    } catch (UncheckedException e) {
      throw e.underlying;
    }
  }

  private TokenIntrospectionSuccessResponse postIntrospection(String bearerToken)
      throws HttpAuthenticationException {
    final var request = new TokenIntrospectionRequest(introspectionEndpoint, credential,
        new BearerAccessToken(bearerToken));
    final HTTPResponse httpResponse;
    try {
      httpResponse = request.toHTTPRequest().send();
    } catch (IOException e) {
      throw new HttpAuthenticationException("The authorization server is unavailable", e,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
    final TokenIntrospectionResponse response;
    try {
      response = TokenIntrospectionResponse.parse(httpResponse);
    } catch (ParseException e) {
      throw new HttpAuthenticationException("Received an invalid response from the authorization server", e,
          HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }
    if (!response.indicatesSuccess()) {
      final var error = response.toErrorResponse().getErrorObject();
      final var wwwAuthenticateHeader = error instanceof TokenSchemeError tokenSchemeError
          ? tokenSchemeError.toWWWAuthenticateHeader() : null;
      throw new HttpAuthenticationException("Failed to introspect the token", error.getHTTPStatusCode(),
          wwwAuthenticateHeader);
    }
    final var result = response.toSuccessResponse();
    if (!result.isActive()) {
      final var error = BearerTokenError.INVALID_TOKEN;
      throw new HttpAuthenticationException("The token is not active", error.getHTTPStatusCode(),
          error.toWWWAuthenticateHeader());
    }
    if (result.getAudience() == null || !result.getAudience().contains(audience)) {
      final var error = BearerTokenError.INVALID_TOKEN;
      throw new HttpAuthenticationException("The aud is invalid: " + result.getAudience(), error.getHTTPStatusCode(),
          error.toWWWAuthenticateHeader());
    }
    // According RFC7662, an authorization server MUST validate the expiration, so this trusts the isActive flag
    return result;
  }
}
