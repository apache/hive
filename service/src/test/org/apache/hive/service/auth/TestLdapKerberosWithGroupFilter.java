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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.auth;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.ldap.DirSearch;
import org.apache.hive.service.auth.ldap.DirSearchFactory;
import org.apache.hive.service.auth.ldap.LdapGroupCallbackHandler;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.AuthorizeCallback;

import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for Kerberos authentication with LDAP group filtering.
 * This test uses mocks to avoid the need for real LDAP or Kerberos servers.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestLdapKerberosWithGroupFilter {

  private static final String GROUP1_NAME = "group1";
  private static final String GROUP2_NAME = "group2";
  private static final String USER1_ID = "user1";
  private static final String USER2_ID = "user2";
  private static final String USER1_PRINCIPAL = USER1_ID + "@TEST.REALM";
  private static final String USER2_PRINCIPAL = USER2_ID + "@TEST.REALM";

  @Mock
  private DirSearch dirSearch;

  @Mock
  private DirSearchFactory dirSearchFactory;

  @Mock
  private CallbackHandler delegateHandler;

  private HiveConf conf;

  @Before
  public void setup() throws Exception {
    conf = new HiveConf();
    conf.set("hive.root.logger", "DEBUG,console");

    // Setup LDAP connection parameters
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_URL, "ldap://localhost:10389");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_USER, "cn=admin,dc=example,dc=com");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_PASSWORD, "admin");

    // Configure Kerberos auth
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION, "KERBEROS");

    // Reset mocks before each test
    reset(dirSearchFactory, dirSearch, delegateHandler);

    // Set up the default mock behavior
    when(dirSearchFactory.getInstance(any(HiveConf.class), anyString(), anyString()))
        .thenReturn(dirSearch);

    doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      if (args.length > 0 && args[0] instanceof Callback[]) {
        for (Callback callback : (Callback[]) args[0]) {
          if (callback instanceof AuthorizeCallback) {
            AuthorizeCallback authorizeCallback = (AuthorizeCallback) callback;
            String authId = authorizeCallback.getAuthenticationID();
            String authzId = authorizeCallback.getAuthorizationID();
            authorizeCallback.setAuthorized(authId != null && authId.equals(authzId));
          }
        }
      }
      return null;
    }).when(delegateHandler).handle(any(Callback[].class));
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testKerberosAuthWithLdapGroupCheckPositive() throws Exception {
    // Configure LDAP to allow only users in group1
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, GROUP1_NAME);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    String userDn = "uid=user1,dc=example,dc=com";
    String groupDn = "cn=group1,dc=example,dc=com";

    // Mock the DirSearch to succeed for both UserSearchFilter and GroupMembershipKeyFilter
    when(dirSearch.findUserDn(USER1_ID)).thenReturn(userDn);
    when(dirSearch.findGroupsForUser(eq(userDn))).thenReturn(Collections.singletonList(groupDn));

    // Create the callback handler with our test configuration
    LdapGroupCallbackHandler callbackHandler = LdapGroupCallbackHandler.createForTesting(
        conf, dirSearchFactory, delegateHandler);

    // Create an AuthorizeCallback as would be done by Kerberos authentication
    AuthorizeCallback ac = new AuthorizeCallback(USER1_PRINCIPAL, USER1_PRINCIPAL);
    Callback[] callbacks = {ac};
    callbackHandler.handle(callbacks);

    assertTrue(ac.isAuthorized());

    // Verify LDAP operations occurred
    verify(dirSearchFactory).getInstance(eq(conf), eq("cn=admin,dc=example,dc=com"), eq("admin")); // More specific
    verify(dirSearch, times(2)).findUserDn(USER1_ID);
    verify(dirSearch).findGroupsForUser(eq(userDn)); // Changed from anyString()

  }

  @Test
  public void testKerberosAuthWithLdapGroupCheckNegative() throws Exception {
    // Configure LDAP to allow only users in group1
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, GROUP1_NAME);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);

    String userDn = "uid=user2,dc=example,dc=com";
    String wrongGroupDn = "cn=group3,dc=example,dc=com";

    when(dirSearch.findUserDn(USER2_ID)).thenReturn(userDn);
    when(dirSearch.findGroupsForUser(eq(userDn))).thenReturn(Collections.singletonList(wrongGroupDn));

    LdapGroupCallbackHandler callbackHandler = LdapGroupCallbackHandler.createForTesting(
        conf, dirSearchFactory, delegateHandler);

    AuthorizeCallback ac = new AuthorizeCallback(USER2_PRINCIPAL, USER2_PRINCIPAL);
    Callback[] callbacks = {ac};
    callbackHandler.handle(callbacks);

    assertFalse(ac.isAuthorized());

    verify(dirSearch, times(2)).findUserDn(USER2_ID);
    verify(dirSearch).findGroupsForUser(eq(userDn));
  }

  @Test
  public void testKerberosAuthWithDisabledLdapGroupCheck() throws Exception {
    // Disable LDAP group check
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, false);
    // Even if a group filter is set, it should be ignored
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, GROUP1_NAME);

    LdapGroupCallbackHandler callbackHandler = LdapGroupCallbackHandler.createForTesting(
        conf, dirSearchFactory, delegateHandler);

    AuthorizeCallback ac1 = new AuthorizeCallback(USER1_PRINCIPAL, USER1_PRINCIPAL);
    AuthorizeCallback ac2 = new AuthorizeCallback(USER2_PRINCIPAL, USER2_PRINCIPAL);

    callbackHandler.handle(new Callback[]{ac1, ac2});

    // Both users should be authorized since group check is disabled
    assertTrue(ac1.isAuthorized());
    assertTrue(ac2.isAuthorized());

    // Ensure no LDAP interactions occurred
    verifyNoInteractions(dirSearch);
  }

}
