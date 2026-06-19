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

import com.nimbusds.oauth2.sdk.GeneralException;
import com.nimbusds.oauth2.sdk.as.AuthorizationServerMetadata;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.id.Issuer;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;

/**
 * A factory to create an {@link OAuth2Authenticator} instance based on the configuration.
 */
public final class OAuth2AuthenticatorFactory {
  private OAuth2AuthenticatorFactory() {
    throw new AssertionError();
  }

  public static OAuth2Authenticator createAuthenticator(Configuration conf) throws IOException {
    final var issuer = Issuer.parse(MetastoreConf.getAsString(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_ISSUER));
    Objects.requireNonNull(issuer);
    final var audience = MetastoreConf.getAsString(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_AUDIENCE);
    final AuthorizationServerMetadata metadata;
    try {
      metadata = AuthorizationServerMetadata.resolve(issuer);
    } catch (GeneralException e) {
      throw new IOException("Failed to resolve the authorization server metadata. " +
          "Please check %s/.well-known/oauth-authorization-server is available".formatted(issuer), e);
    }

    final var claim = MetastoreConf.getAsString(conf,
        ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_PRINCIPAL_MAPPER_REGEX_FIELD);
    final var pattern = Pattern.compile(MetastoreConf.getAsString(conf,
            ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_PRINCIPAL_MAPPER_REGEX_PATTERN));
    final var principalMapper = new RegexOAuth2PrincipalMapper(claim, pattern);

    final var validation = MetastoreConf.getAsString(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_VALIDATION_METHOD);
    switch (validation) {
    case "jwt" -> {
      if (metadata.getJWKSetURI() == null) {
        throw new IllegalStateException(".well-known/oauth-authorization-server does not include jwks_uri");
      }
      return new JWTAccessTokenAuthenticator(issuer, metadata.getJWKSetURI().toURL(), audience, principalMapper);
    }
    case "introspection" -> {
      if (metadata.getIntrospectionEndpointURI() == null) {
        throw new IllegalStateException(
            ".well-known/oauth-authorization-server does not include introspection_endpoint");
      }
      // RFC7662 does not specify any standard way to authenticate the HTTP client. At this moment, we use the client
      // authentication, which is the most common one
      final var clientId = MetastoreConf.getAsString(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_CLIENT_ID);
      final var clientSecret = MetastoreConf.getPassword(conf, ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_CLIENT_SECRET);
      final var credential = new ClientSecretBasic(new ClientID(clientId), new Secret(clientSecret));
      final var cacheExpiry = Duration.ofSeconds(MetastoreConf.getTimeVar(conf,
          MetastoreConf.ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_INTROSPECTION_CACHE_EXPIRY, TimeUnit.SECONDS));
      final var cacheSize = MetastoreConf.getLongVar(conf,
          ConfVars.CATALOG_SERVLET_AUTH_OAUTH2_INTROSPECTION_CACHE_SIZE);
      return new TokenIntrospectionAuthenticator(metadata.getIntrospectionEndpointURI(), new Audience(audience),
          credential, principalMapper, cacheExpiry, cacheSize);
    }
    default -> throw new IllegalArgumentException("Illegal validation method: " + validation);
    }
  }
}
