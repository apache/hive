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

import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.auth.HttpAuthenticationException;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.keycloak.OAuth2Constants;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.ClientScopeRepresentation;
import org.keycloak.representations.idm.ProtocolMapperRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

@Category(MetastoreUnitTest.class)
public class TestTokenIntrospectionAuthenticator {
  private static final String AUDIENCE = "http://localhost:8081/hive";
  private static final String USERNAME = "test-user";
  private static final List<String> SCOPES = List.of("read", "update");
  private static final ClientSecretBasic RESOURCE_SERVER_CREDENTIAL = new ClientSecretBasic(
      new ClientID("hive-resource-server-id"), new Secret("hive-resource-server-secret"));
  private static final OAuth2PrincipalMapper PRINCIPAL_MAPPER = new RegexOAuth2PrincipalMapper("email",
      Pattern.compile("(.*)@example.com"));

  private static GenericContainer<?> container;
  private static URI introspectionEndpoint;
  private static String accessToken;
  private static String accessTokenExpired;
  private static String accessTokenWithWrongIssuer;
  private static String accessTokenWithWrongAudience;
  private static String accessTokenWithMissingScope;
  private static String accessTokenWithInsufficientScope;

  private static RealmResource createRealm(Keycloak keycloak, String realmName) {
    var realm = new RealmRepresentation();
    realm.setRealm(realmName);
    realm.setEnabled(true);
    keycloak.realms().create(realm);
    return keycloak.realm(realmName);
  }

  private static void createResourceServer(RealmResource realm) {
    var resourceServer = new ClientRepresentation();
    resourceServer.setClientId(RESOURCE_SERVER_CREDENTIAL.getClientID().getValue());
    resourceServer.setSecret(RESOURCE_SERVER_CREDENTIAL.getClientSecret().getValue());
    resourceServer.setEnabled(true);
    resourceServer.setProtocol("openid-connect");
    resourceServer.setPublicClient(false);
    resourceServer.setServiceAccountsEnabled(true);
    resourceServer.setAuthorizationServicesEnabled(true);
    realm.clients().create(resourceServer).close();
  }

  private static void createScope(RealmResource realm, String name) {
    var scope = new ClientScopeRepresentation();
    scope.setName(name);
    scope.setProtocol("openid-connect");
    realm.clientScopes().create(scope).close();
  }

  private static ProtocolMapperRepresentation createAudience(String audience) {
    var aud = new ProtocolMapperRepresentation();
    aud.setName("audience");
    aud.setProtocol("openid-connect");
    aud.setProtocolMapper("oidc-audience-mapper");
    aud.setConfig(Map.of(
        "included.custom.audience", audience,
        "access.token.claim", "true"
    ));
    return aud;
  }

  private static ProtocolMapperRepresentation createEmailClaim() {
    var mapper = new ProtocolMapperRepresentation();
    mapper.setName("email");
    mapper.setProtocol("openid-connect");
    mapper.setProtocolMapper("oidc-hardcoded-claim-mapper");
    mapper.setConfig(Map.of(
        "claim.name", "email",
        "claim.value", USERNAME + "@example.com",
        "jsonType.label", "String",
        "access.token.claim", "true"
    ));
    return mapper;
  }

  private static void createClient(RealmResource realm, String clientId, String clientSecret, List<String> scopes,
      List<ProtocolMapperRepresentation> protocolMappers) {
    createClient(realm, clientId, clientSecret, scopes, protocolMappers, Collections.emptyMap());
  }

  private static void createClient(RealmResource realm, String clientId, String clientSecret, List<String> scopes,
      List<ProtocolMapperRepresentation> protocolMappers, Map<String, String> additionalAttributes) {
    var client = new ClientRepresentation();
    client.setClientId(clientId);
    client.setSecret(clientSecret);
    client.setEnabled(true);
    client.setProtocol("openid-connect");
    client.setPublicClient(false);
    client.setServiceAccountsEnabled(true);
    client.setOptionalClientScopes(scopes);
    var attributes = new HashMap<>(additionalAttributes);
    attributes.put("access.token.header.type.rfc9068", "true");
    client.setAttributes(attributes);
    client.setProtocolMappers(protocolMappers);
    realm.clients().create(client).close();
  }

  private static String getAccessToken(String url, String realm, String clientId, String clientSecret,
      List<String> scopes) {
    try (var keycloak = KeycloakBuilder.builder()
        .serverUrl(url)
        .realm(realm)
        .clientId(clientId)
        .clientSecret(clientSecret)
        .scope(scopes == null ? null : String.join(" ", scopes))
        .grantType(OAuth2Constants.CLIENT_CREDENTIALS)
        .build()) {
      return keycloak.tokenManager().getAccessTokenString();
    }
  }

  @BeforeClass
  public static void setup() throws URISyntaxException {
    container = new GenericContainer<>(DockerImageName.parse("quay.io/keycloak/keycloak:26.3.4"))
        .withEnv("KEYCLOAK_ADMIN", "admin")
        .withEnv("KEYCLOAK_ADMIN_PASSWORD", "admin")
        .withCommand("start-dev")
        .withExposedPorts(8080)
        .withStartupTimeout(Duration.ofMinutes(5));
    container.start();
    var base = "http://%s:%d".formatted(container.getHost(), container.getMappedPort(8080));
    var keycloak = Keycloak.getInstance(base, "master", "admin", "admin", "admin-cli");

    var realmName = "hive";
    var realm = createRealm(keycloak, realmName);
    var wrongRealmName = "hive-another";
    var wrongRealm = createRealm(keycloak, wrongRealmName);
    introspectionEndpoint = new URI("%s/realms/hive/protocol/openid-connect/token/introspect".formatted(base));

    createResourceServer(realm);
    createResourceServer(wrongRealm);

    for (String scope : List.of("read", "update", "delete")) {
      createScope(realm, scope);
      createScope(wrongRealm, scope);
    }
    var audience = createAudience(AUDIENCE);
    var wrongAudience = createAudience("http://localhost:8080/wrong");
    var email = createEmailClaim();

    var clientId = "test-client-id";
    var clientSecret = "test-client-secret";
    createClient(realm, clientId, clientSecret, List.of("read", "update", "delete"),
        List.of(audience, email));
    createClient(wrongRealm, clientId, clientSecret, List.of("read", "update", "delete"),
        List.of(audience, email));
    var expiredClientId = "expired-client-id";
    var expiredClientSecret = "expired-client-secret";
    createClient(realm, expiredClientId, expiredClientSecret, List.of("read", "update", "delete"),
        List.of(audience, email), Collections.singletonMap("access.token.lifespan", "1"));
    var wrongAudienceClientId = "wrong-audience-id";
    var wrongAudienceClientSecret = "wrong-audience-secret";
    createClient(realm, wrongAudienceClientId, wrongAudienceClientSecret,
        List.of("read", "update", "delete"), List.of(wrongAudience, email));

    accessToken = getAccessToken(base, realmName, clientId, clientSecret, SCOPES);
    accessTokenExpired = getAccessToken(base, realmName, expiredClientId, expiredClientSecret, SCOPES);
    accessTokenWithWrongIssuer = getAccessToken(base, wrongRealmName, clientId, clientSecret, SCOPES);
    accessTokenWithWrongAudience = getAccessToken(base, realmName, wrongAudienceClientId, wrongAudienceClientSecret,
        SCOPES);
    accessTokenWithMissingScope = getAccessToken(base, realmName, clientId, clientSecret, null);
    accessTokenWithInsufficientScope = getAccessToken(base, realmName, clientId, clientSecret,
        List.of("read", "delete"));
  }

  @AfterClass
  public static void teardown() {
    if (container != null) {
      container.stop();
    }
  }

  @Test
  public void testSuccess() throws HttpAuthenticationException {
    var authenticator = new TokenIntrospectionAuthenticator(introspectionEndpoint, new Audience(AUDIENCE),
        RESOURCE_SERVER_CREDENTIAL, PRINCIPAL_MAPPER, Duration.ofMinutes(1), 10);
    var actual = authenticator.resolveUserName(accessToken, SCOPES);
    Assert.assertEquals(USERNAME, actual);
  }

  @Test
  public void testExpired() {
    var authenticator = new TokenIntrospectionAuthenticator(introspectionEndpoint, new Audience(AUDIENCE),
        RESOURCE_SERVER_CREDENTIAL, PRINCIPAL_MAPPER, Duration.ofMinutes(0), 0);
    Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
      var error = Assert.assertThrows(HttpAuthenticationException.class,
          () -> authenticator.resolveUserName(accessTokenExpired, SCOPES));
      Assert.assertEquals("The token is not active", error.getMessage());
      Assert.assertEquals(401, error.getStatusCode());
      Assert.assertEquals(Optional.of(
          "Bearer error=\"invalid_token\", error_description=\"Invalid access token\""),
          error.getWwwAuthenticateHeader());
    });
  }

  @Test
  public void testNullToken() {
    var authenticator = new TokenIntrospectionAuthenticator(introspectionEndpoint, new Audience(AUDIENCE),
        RESOURCE_SERVER_CREDENTIAL, PRINCIPAL_MAPPER, Duration.ofMinutes(1), 10);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(null, SCOPES));
    Assert.assertEquals("Missing bearer token", error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of("Bearer"), error.getWwwAuthenticateHeader());
  }

  @Test
  public void testWrongIssuer() {
    var authenticator = new TokenIntrospectionAuthenticator(introspectionEndpoint, new Audience(AUDIENCE),
        RESOURCE_SERVER_CREDENTIAL, PRINCIPAL_MAPPER, Duration.ofMinutes(1), 10);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(accessTokenWithWrongIssuer, SCOPES));
    Assert.assertEquals("The token is not active", error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"invalid_token\", error_description=\"Invalid access token\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testWrongAudience() {
    var authenticator = new TokenIntrospectionAuthenticator(introspectionEndpoint, new Audience(AUDIENCE),
        RESOURCE_SERVER_CREDENTIAL, PRINCIPAL_MAPPER, Duration.ofMinutes(1), 10);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(accessTokenWithWrongAudience, SCOPES));
    Assert.assertEquals("The aud is invalid: [http://localhost:8080/wrong]",
        error.getMessage());
    Assert.assertEquals(401, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"invalid_token\", error_description=\"Invalid access token\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testMissingScope() {
    var authenticator = new TokenIntrospectionAuthenticator(introspectionEndpoint, new Audience(AUDIENCE),
        RESOURCE_SERVER_CREDENTIAL, PRINCIPAL_MAPPER, Duration.ofMinutes(1), 10);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(accessTokenWithMissingScope, SCOPES));
    Assert.assertEquals("This resource requires the following scopes: [read, update]",
        error.getMessage());
    Assert.assertEquals(403, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"insufficient_scope\", error_description=\"Insufficient scope\", scope=\"read update\""),
        error.getWwwAuthenticateHeader());
  }

  @Test
  public void testInsufficientScope() {
    var authenticator = new TokenIntrospectionAuthenticator(introspectionEndpoint, new Audience(AUDIENCE),
        RESOURCE_SERVER_CREDENTIAL, PRINCIPAL_MAPPER, Duration.ofMinutes(1), 10);
    var error = Assert.assertThrows(HttpAuthenticationException.class,
        () -> authenticator.resolveUserName(accessTokenWithInsufficientScope, SCOPES));
    Assert.assertEquals("Insufficient scopes: [update]", error.getMessage());
    Assert.assertEquals(403, error.getStatusCode());
    Assert.assertEquals(Optional.of(
            "Bearer error=\"insufficient_scope\", error_description=\"Insufficient scope\", scope=\"read update\""),
        error.getWwwAuthenticateHeader());
  }
}
