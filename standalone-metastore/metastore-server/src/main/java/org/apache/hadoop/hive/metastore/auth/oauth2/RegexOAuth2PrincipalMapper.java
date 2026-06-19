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

import com.nimbusds.oauth2.sdk.token.BearerTokenError;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;

/**
 * The regex-based principal mapper.
 */
public class RegexOAuth2PrincipalMapper implements OAuth2PrincipalMapper {
  private final String claimName;
  private final Pattern pattern;

  RegexOAuth2PrincipalMapper(String claimName, Pattern pattern) {
    this.claimName = claimName;
    this.pattern = pattern;
  }

  @Override
  public String getUserName(ClaimProvider rawValueProvider) throws HttpAuthenticationException {
    final var rawValue = rawValueProvider.provide(claimName);
    if (rawValue == null) {
      final var error = BearerTokenError.INVALID_TOKEN;
      throw new HttpAuthenticationException(
          "Authentication error: Claim '%s' not found in token".formatted(claimName),
          error.getHTTPStatusCode(), error.toWWWAuthenticateHeader());
    }
    final var matcher = pattern.matcher(rawValue);
    if (!matcher.find()) {
      final var error = BearerTokenError.INVALID_TOKEN;
      throw new HttpAuthenticationException(
          "Authentication error: Claim '%s' does not match %s".formatted(claimName, pattern.pattern()),
          error.getHTTPStatusCode(), error.toWWWAuthenticateHeader());
    }
    if (matcher.groupCount() != 1) {
      throw new IllegalStateException("Pattern must extract exactly one group, but %s picked up %d groups".formatted(
          pattern.pattern(), matcher.groupCount()));
    }
    return matcher.group(1);
  }
}
