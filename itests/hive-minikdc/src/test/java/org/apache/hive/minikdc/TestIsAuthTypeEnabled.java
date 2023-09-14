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

package org.apache.hive.minikdc;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.HttpAuthUtils;
import org.apache.hive.service.auth.saml.HiveSamlUtils;
import org.apache.hive.service.cli.thrift.ThriftHttpServlet;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static org.apache.hive.service.auth.HiveAuthConstants.AuthTypes;
import static org.apache.hive.service.cli.thrift.ThriftHttpServlet.HIVE_DELEGATION_TOKEN_HEADER;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the logic that determines whether a given authentication mode
 * should be checked based on the Authorization header.
 */
public class TestIsAuthTypeEnabled {

  private static final int MOCK_JWKS_SERVER_PORT = 8089;
  private static final File jwtVerificationJWKSFile =
    new File("src/test/resources/auth.jwt/jwt-verification-jwks.json");

  @ClassRule
  public static final WireMockRule MOCK_JWKS_SERVER = new WireMockRule(MOCK_JWKS_SERVER_PORT);

  @Before
  public void setUp() throws Exception {
    MOCK_JWKS_SERVER.stubFor(get("/jwks")
      .willReturn(ok()
        .withBody(Files.readAllBytes(jwtVerificationJWKSFile.toPath()))));
  }

  @Test
  public void testIsAuthTypeEnabled() throws Exception {

    assertTrue(
      // isAuthTypeEnabled should be true when Kerberos authentication is enabled
      // and Kerberos authentication is initiated with "Authorization: Negotiate ..." header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.LDAP, AuthTypes.KERBEROS, AuthTypes.SAML)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HttpAuthUtils.AUTHORIZATION,
          Optional.of("Negotiate anyToken"))),
        AuthTypes.KERBEROS
      )
    );

    assertFalse(
      // isAuthTypeEnabled should be false when Kerberos authentication is disabled
      // and Kerberos authentication is initiated with "Authorization: Negotiate ..." header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.SAML, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HttpAuthUtils.AUTHORIZATION,
          Optional.of("Negotiate anyToken"))),
        AuthTypes.KERBEROS
      )
    );

    assertTrue(
      // isAuthTypeEnabled should be true when Kerberos authentication is enabled
      // and delegation token received in X-Hive-Delegation-Token header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.LDAP, AuthTypes.KERBEROS, AuthTypes.SAML)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HIVE_DELEGATION_TOKEN_HEADER,
          Optional.of("any Hive delegation token"))),
        AuthTypes.KERBEROS
      )
    );

    assertFalse(
      // isAuthTypeEnabled should be false when Kerberos authentication is disabled
      // and delegation token received in X-Hive-Delegation-Token header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.SAML, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HIVE_DELEGATION_TOKEN_HEADER,
          Optional.of("any Hive delegation token"))),
        AuthTypes.KERBEROS
      )
    );

    assertTrue(
      // isAuthTypeEnabled should be true when JWT authentication is enabled
      // and JWT authentication is initiated with "Authorization: Bearer jwt...token" header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.KERBEROS, AuthTypes.JWT, AuthTypes.SAML, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HttpAuthUtils.AUTHORIZATION,
          Optional.of("Bearer any.jwt.token"))),
        AuthTypes.JWT
      )
    );

    assertFalse(
      // isAuthTypeEnabled should be false when JWT authentication is disabled
      // and JWT authentication is initiated with "Authorization: Bearer jwt...token" header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.KERBEROS, AuthTypes.SAML, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HttpAuthUtils.AUTHORIZATION,
          Optional.of("Bearer any.jwt.token"))),
        AuthTypes.JWT
      )
    );

    assertTrue(
      // isAuthTypeEnabled should be true when SAML authentication is enabled
      // and SAML authentication is initiated with "Authorization: Bearer saml.token" header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.KERBEROS, AuthTypes.SAML, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HttpAuthUtils.AUTHORIZATION,
          Optional.of("Bearer anySamlToken"))),
        AuthTypes.SAML
      )
    );

    assertFalse(
      // isAuthTypeEnabled should be false when SAML authentication is disabled
      // and SAML authentication is initiated with "Authorization: Bearer saml.token" header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.KERBEROS, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HttpAuthUtils.AUTHORIZATION,
          Optional.of("Bearer anySamlToken"))),
        AuthTypes.SAML
      )
    );

    assertTrue(
      // isAuthTypeEnabled should be true when SAML authentication is enabled,
      // no "Authorization" header received, but X-Hive-Token-Response-Port header
      // indicates SAML authentication
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.KERBEROS, AuthTypes.SAML, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT,
          Optional.of("4321"))),
        AuthTypes.SAML
      )
    );

    assertFalse(
      // isAuthTypeEnabled should be false when SAML authentication is disabled,
      // no "Authorization" header received and X-Hive-Token-Response-Port header
      // is present
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.KERBEROS, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HiveSamlUtils.SSO_TOKEN_RESPONSE_PORT,
          Optional.of("4321"))),
        AuthTypes.SAML
      )
    );

    assertTrue(
      // isAuthTypeEnabled should be true when LDAP authentication is enabled
      // and LDAP authentication is initiated with "Authorization: Basic ..." header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.KERBEROS, AuthTypes.SAML, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HttpAuthUtils.AUTHORIZATION,
          Optional.of("Basic base64EncodedCredentials"))),
        AuthTypes.LDAP
      )
    );

    assertFalse(
      // isAuthTypeEnabled should be false when LDAP authentication is disabled
      // and LDAP authentication is initiated with "Authorization: Basic ..." header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.KERBEROS, AuthTypes.SAML)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(ImmutableMap.of(HttpAuthUtils.AUTHORIZATION,
          Optional.of("Basic base64EncodedCredentials"))),
        AuthTypes.LDAP
      )
    );

    assertFalse(
      // isAuthTypeEnabled should be false when JWT authentication is enabled
      // but there is no JWT token in the Authorization header
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.JWT, AuthTypes.SAML, AuthTypes.LDAP)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(),
        AuthTypes.JWT
      )
    );

    assertFalse(
      // isAuthTypeEnabled should be false when JWT authentication is enabled
      // but checking Kerberos authentication
      createThriftHttpServletWithAuthTypes(
        ImmutableList.of(AuthTypes.JWT, AuthTypes.SAML, AuthTypes.LDAP, AuthTypes.KERBEROS)
      ).isAuthTypeEnabled(
        createMockHttpServletRequest(),
        AuthTypes.KERBEROS
      )
    );

  }

  private HttpServletRequest createMockHttpServletRequest() {
    return createMockHttpServletRequest(ImmutableMap.of());
  }

  private HttpServletRequest createMockHttpServletRequest(Map<String, Optional<String>> headers) {
    HttpServletRequest httpServletRequest = Mockito.mock(HttpServletRequest.class);
    headers.entrySet().stream().forEach(
      entry -> Mockito
        .when(httpServletRequest.getHeader(entry.getKey()))
        .thenReturn(entry.getValue().orElse(null))
    );
    return httpServletRequest;
  }

  private ThriftHttpServlet createThriftHttpServletWithAuthTypes(List<AuthTypes> authTypesList)
          throws Exception {
    HiveConf hiveConf = new HiveConf();
    String authType = authTypesList.stream().map(Object::toString)
      .collect(Collectors.joining(","));
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, authType);
    // the content of the URL below is the same as jwtVerificationJWKSFile
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION_JWT_JWKS_URL,
  "http://localhost:" + MOCK_JWKS_SERVER_PORT + "/jwks");
    return new ThriftHttpServlet(null, null, null, null, null, hiveConf);
  }
}
