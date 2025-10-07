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

package org.apache.iceberg.rest.extension;


import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.keycloak.OAuth2Constants;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.ClientRepresentation;
import org.keycloak.representations.idm.ClientScopeRepresentation;
import org.keycloak.representations.idm.ProtocolMapperRepresentation;
import org.keycloak.representations.idm.RealmRepresentation;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public class OAuth2AuthorizationServer {
  private static final String REALM = "hive";
  static final String HMS_ID = "hive-metastore";
  static final String HMS_SECRET = "hive-metastore-secret";
  private static final String ICEBERG_CLIENT_ID = "iceberg-client";
  private static final String ICEBERG_CLIENT_SECRET = "iceberg-client-secret";

  private GenericContainer<?> container;
  private Keycloak keycloak;
  private String issuer;
  private String tokenEndpoint;
  private String accessToken;
  private final boolean accessTokenHeaderTypeRfc9068;
  private final Network dockerNetwork;
  
  public OAuth2AuthorizationServer(Network dockerNetwork, boolean accessTokenHeaderTypeRfc9068) {
    this.dockerNetwork = dockerNetwork;
    this.accessTokenHeaderTypeRfc9068 = accessTokenHeaderTypeRfc9068;
  }

  public OAuth2AuthorizationServer() {
    dockerNetwork = Network.newNetwork();
    this.accessTokenHeaderTypeRfc9068 = true;
  }

  private static RealmResource createRealm(Keycloak keycloak) {
    var realm = new RealmRepresentation();
    realm.setRealm(REALM);
    realm.setEnabled(true);
    keycloak.realms().create(realm);
    return keycloak.realm(REALM);
  }

  private static void createResourceServer(RealmResource realm) {
    var resourceServer = new ClientRepresentation();
    resourceServer.setClientId(HMS_ID);
    resourceServer.setSecret(HMS_SECRET);
    resourceServer.setEnabled(true);
    resourceServer.setProtocol("openid-connect");
    resourceServer.setPublicClient(false);
    resourceServer.setServiceAccountsEnabled(true);
    resourceServer.setAuthorizationServicesEnabled(true);
    realm.clients().create(resourceServer).close();
  }

  private static void createScope(RealmResource realm) {
    var scope = new ClientScopeRepresentation();
    scope.setName("catalog");
    scope.setProtocol("openid-connect");
    realm.clientScopes().create(scope).close();
  }

  private static ProtocolMapperRepresentation createAudience() {
    var aud = new ProtocolMapperRepresentation();
    aud.setName("audience");
    aud.setProtocol("openid-connect");
    aud.setProtocolMapper("oidc-audience-mapper");
    aud.setConfig(Map.of(
        "included.custom.audience", HMS_ID,
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
        "claim.value", "iceberg-user@example.com",
        "jsonType.label", "String",
        "access.token.claim", "true"
    ));
    return mapper;
  }

  private void createClient(RealmResource realm, List<String> scopes,
      List<ProtocolMapperRepresentation> protocolMappers) {
    var client = new ClientRepresentation();
    client.setClientId(ICEBERG_CLIENT_ID);
    client.setSecret(ICEBERG_CLIENT_SECRET);
    client.setEnabled(true);
    client.setProtocol("openid-connect");
    client.setPublicClient(false);
    client.setServiceAccountsEnabled(true);
    client.setOptionalClientScopes(scopes);
    client.setAttributes(Collections.singletonMap("access.token.header.type.rfc9068",
        Boolean.valueOf(accessTokenHeaderTypeRfc9068).toString()));
    client.setProtocolMappers(protocolMappers);
    realm.clients().create(client).close();
  }

  private static String getAccessToken(String url, List<String> scopes) {
    try (var keycloak = KeycloakBuilder.builder()
        .serverUrl(url)
        .realm(REALM)
        .clientId(ICEBERG_CLIENT_ID)
        .clientSecret(ICEBERG_CLIENT_SECRET)
        .scope(String.join(" ", scopes))
        .grantType(OAuth2Constants.CLIENT_CREDENTIALS)
        .build()) {
      return keycloak.tokenManager().getAccessTokenString();
    }
  }

  public void start() {
    container = new GenericContainer<>(DockerImageName.parse("quay.io/keycloak/keycloak:26.3.4"))
        .withEnv("KEYCLOAK_ADMIN", "admin")
        .withEnv("KEYCLOAK_ADMIN_PASSWORD", "admin")
        .withCommand("start-dev")
        .withExposedPorts(8080)
        .withNetwork(dockerNetwork)
        .withStartupTimeout(Duration.ofMinutes(5));
    container.start();

    var base = "http://%s:%d".formatted(container.getHost(), container.getMappedPort(8080));
    keycloak = Keycloak.getInstance(base, "master", "admin", "admin", "admin-cli");

    var realm = createRealm(keycloak);
    createResourceServer(realm);
    issuer = "%s/realms/%s".formatted(base, REALM);
    tokenEndpoint = "%s/protocol/openid-connect/token".formatted(issuer);

    createScope(realm);
    var audience = createAudience();
    var email = createEmailClaim();
    createClient(realm, List.of("catalog"), List.of(audience, email));
    accessToken = getAccessToken(base, List.of("catalog"));
  }

  public void stop() {
    if (container != null) {
      container.stop();
      keycloak.close();
    }
  }

  public String getIssuer() {
    return issuer;
  }

  public String getTokenEndpoint() {
    return tokenEndpoint;
  }

  public String getClientCredential() {
    return "%s:%s".formatted(ICEBERG_CLIENT_ID, ICEBERG_CLIENT_SECRET);
  }

  public String getAccessToken() {
    return accessToken;
  }
  
  public String getKeycloackContainerDockerInternalHostName() {
    return container.getNetworkAliases().get(0);
  }
}
