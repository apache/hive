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
package org.apache.hive.service.cli.thrift;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HttpAuthenticationException;
import org.apache.hive.service.auth.ldap.DirSearch;
import org.apache.hive.service.auth.ldap.DirSearchFactory;
import org.apache.hive.service.auth.ldap.KerberosLdapFilterEnforcer;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.security.sasl.AuthenticationException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Tests for the HTTP Kerberos authentication with additional LDAP group filtering.
 * Uses Mockito for mocking LDAP and Kerberos components.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestThriftHttpKerberosLdapFilter {

  private static final String TEST_USER = "user";
  private static final String TEST_REALM = "TEST.REALM";
  private static final String TEST_PRINCIPAL = TEST_USER + "@" + TEST_REALM;

  @Mock
  private GSSContext gssContext;

  @Mock
  private GSSName gssName;

  @Mock
  private DirSearch dirSearch;

  @Mock
  private DirSearchFactory dirSearchFactory;

  private HiveConf hiveConf;
  private TestableKerberosAuthHandler authHandler;

  @Before
  public void setup() throws Exception {
    hiveConf = new HiveConf();

    // Set up bind user and password
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_USER, "bindUser");
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_PASSWORD, "bindPassword");

    // Mock GSSContext to return a test principal
    when(gssName.toString()).thenReturn(TEST_PRINCIPAL);
    when(gssContext.getSrcName()).thenReturn(gssName);

    // Mock DirSearchFactory
    when(dirSearchFactory.getInstance(any(HiveConf.class), anyString(), anyString())).thenReturn(dirSearch);

    authHandler = null;
  }

  private void createAuthHandler() {
    authHandler = new TestableKerberosAuthHandler(hiveConf, dirSearchFactory);
  }

  @Test
  public void testKerberosAuthWithNoLdapFilters() throws Exception {
    // Disable filters
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, false);

    createAuthHandler();
    // Run authentication
    String username = authHandler.run();

    assertEquals(TEST_USER, username);
    verifyNoInteractions(dirSearch);
  }

  @Test
  public void testKerberosAuthWithGroupFilterEnabled() throws Exception {
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "group1,group2");

    String userDn = "uid=user,dc=example,dc=com";
    String groupDn = "cn=group1,dc=example,dc=com";

    when(dirSearch.findUserDn(TEST_USER)).thenReturn(userDn);
    when(dirSearch.findGroupsForUser(eq(userDn))).thenReturn(Collections.singletonList(groupDn));

    createAuthHandler();
    String username = authHandler.run();
    assertEquals(TEST_USER, username);

    verify(dirSearchFactory).getInstance(eq(hiveConf), eq("bindUser"), eq("bindPassword"));
    verify(dirSearch, times(2)).findUserDn(TEST_USER);
    verify(dirSearch).findGroupsForUser(eq(userDn));
  }

  @Test(expected = HttpAuthenticationException.class)
  public void testKerberosAuthWithGroupFilterFailure() throws Exception {
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "group1");

    String userDn = "uid=user,dc=example,dc=com";
    String wrongGroupDn = "cn=group3,dc=example,dc=com";

    when(dirSearch.findUserDn(TEST_USER)).thenReturn(userDn);
    when(dirSearch.findGroupsForUser(eq(userDn))).thenReturn(Collections.singletonList(wrongGroupDn));

    createAuthHandler();
    authHandler.run();
  }

  @Test
  public void testKerberosAuthWithUserGroupSearchFilter() throws Exception {
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);

    String userSearchFilter = "(&(uid={0})(objectClass=person))";
    String baseDn = "dc=example,dc=com";
    String groupSearchFilter = "(&(memberUid={0})(objectClass=posixGroup))";
    String groupBaseDn = "ou=groups,dc=example,dc=com";
    String userDn = "uid=user,dc=example,dc=com";

    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERSEARCHFILTER, userSearchFilter);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN, baseDn);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPSEARCHFILTER, groupSearchFilter);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPBASEDN, groupBaseDn);

    when(dirSearch.findUserDn(eq(TEST_USER), eq(userSearchFilter), eq(baseDn))).thenReturn(userDn);
    when(dirSearch.executeUserAndGroupFilterQuery(
        eq(TEST_USER),
        eq(userDn),
        eq(groupSearchFilter),
        eq(groupBaseDn)))
        .thenReturn(Collections.singletonList("cn=group1,ou=groups,dc=example,dc=com"));

    createAuthHandler();
    String username = authHandler.run();
    assertEquals(TEST_USER, username);

    verify(dirSearchFactory).getInstance(eq(hiveConf), eq("bindUser"), eq("bindPassword"));
    verify(dirSearch).findUserDn(eq(TEST_USER), eq(userSearchFilter), eq(baseDn));
  }

  @Test(expected = HttpAuthenticationException.class)
  public void testKerberosAuthWithNoFilterConfigured() throws Exception {
    // Enable group check but don't configure any filters
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    // No filters configured - resolveFilter will return null

    createAuthHandler();
    authHandler.run();
  }

  @Test(expected = HttpAuthenticationException.class)
  public void testKerberosAuthWithMissingBindCredentials() throws Exception {
    // Enable group check but remove bind credentials
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "group1");
    hiveConf.unset(ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_USER.varname);

    createAuthHandler();
    // Run authentication - should throw exception
    authHandler.run();
  }

  @Test
  public void testKerberosAuthWithProxyUser() throws Exception {
    // Enable group check
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "group1");

    String userDn = "uid=user,dc=example,dc=com";
    String groupDn = "cn=group1,dc=example,dc=com";

    when(dirSearch.findUserDn(TEST_USER)).thenReturn(userDn);
    when(dirSearch.findGroupsForUser(eq(userDn))).thenReturn(Collections.singletonList(groupDn));

    try {
      org.apache.hive.service.cli.session.SessionManager.setProxyUserName("proxyUser");

      createAuthHandler();
      String username = authHandler.run();

      assertEquals(TEST_USER, username);
      verify(dirSearchFactory).getInstance(eq(hiveConf), eq("bindUser"), eq("bindPassword"));
      verify(dirSearch, times(2)).findUserDn(TEST_USER);
      verify(dirSearch).findGroupsForUser(eq(userDn));
    } finally {
      org.apache.hive.service.cli.session.SessionManager.clearProxyUserName();
    }
  }

  @Test(expected = HttpAuthenticationException.class)
  public void testKerberosAuthWithDirSearchException() throws Exception {
    // Enable group check
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "group1");

    // Configure dirSearchFactory to throw exception
    when(dirSearchFactory.getInstance(any(), anyString(), anyString()))
        .thenThrow(new AuthenticationException("LDAP connection failed"));

    createAuthHandler();
    // Run authentication - should throw exception
    authHandler.run();
  }

  @Test(expected = HttpAuthenticationException.class)
  public void testKerberosAuthWithGssException() throws Exception {
    // Configure GSSContext to throw exception
    when(gssContext.getSrcName()).thenThrow(new GSSException(GSSException.FAILURE));

    createAuthHandler();
    // Run authentication - should throw exception
    authHandler.run();
  }

  @Test(expected = HttpAuthenticationException.class)
  public void testKerberosAuthWithNullGssName() throws Exception {
    // Configure GSSContext to return null name
    when(gssContext.getSrcName()).thenReturn(null);

    createAuthHandler();
    // Run authentication - should throw exception
    authHandler.run();
  }

  /**
   * A custom implementation for testing Kerberos authentication with LDAP filters
   */
  private class TestableKerberosAuthHandler {
    private final HiveConf hiveConf;
    private final DirSearchFactory mockDirSearchFactory;
    private final KerberosLdapFilterEnforcer filterEnforcer;

    public TestableKerberosAuthHandler(HiveConf hiveConf,
                                       DirSearchFactory dirSearchFactory) {
      this.hiveConf = hiveConf;
      this.mockDirSearchFactory = dirSearchFactory;
      this.filterEnforcer = new KerberosLdapFilterEnforcer(hiveConf, dirSearchFactory);
    }

    private void enforceLdapFilters(String principal) throws HttpAuthenticationException {
      // Implementation that delegates to KerberosLdapFilterEnforcer
      boolean enableGroupCheck = hiveConf.getBoolVar(
          HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS);

      if (!enableGroupCheck) {
        return;
      }

      boolean authorized = filterEnforcer.applyLdapFilter(principal);
      if (!authorized) {
        throw new HttpAuthenticationException("LDAP filter check failed for user " + principal);
      }
    }

    public String run() throws HttpAuthenticationException {
      try {
        // Simulate successful GSS context establishment
        if (gssContext == null) {
          throw new HttpAuthenticationException("GSS Context is null");
        }

        // Get principal name from GSS context
        GSSName srcName = gssContext.getSrcName();
        if (srcName == null) {
          throw new HttpAuthenticationException("Kerberos authentication failed: Could not obtain user principal from GSS Context");
        }

        String principal = srcName.toString();
        String shortName = KerberosLdapFilterEnforcer.extractUserName(principal);
        enforceLdapFilters(principal);

        return shortName;
      } catch (GSSException e) {
        throw new HttpAuthenticationException("Kerberos authentication failed", e);
      }
    }
  }
}
