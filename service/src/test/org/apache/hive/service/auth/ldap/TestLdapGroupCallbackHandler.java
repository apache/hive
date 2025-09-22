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
package org.apache.hive.service.auth.ldap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import java.util.Collections;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestLdapGroupCallbackHandler {

  private static final String TEST_USER = "user";
  private static final String TEST_PRINCIPAL = TEST_USER + "@TEST.REALM";

  @Mock
  private DirSearch dirSearch;

  @Mock
  private DirSearchFactory dirSearchFactory;

  @Mock
  private javax.security.auth.callback.CallbackHandler delegateHandler;

  private HiveConf conf;
  private LdapGroupCallbackHandler callbackHandler;

  @Before
  public void setup() throws Exception {
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_USER, "bindUser");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_PASSWORD, "bindPassword");
    when(dirSearchFactory.getInstance(any(HiveConf.class), anyString(), anyString()))
        .thenReturn(dirSearch);
  }

  @Test
  public void testAuthorizeWithNoLdapFilter() throws Exception {
    // Disable LDAP filter check
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, false);

    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);

    AuthorizeCallback ac = new AuthorizeCallback(TEST_PRINCIPAL, TEST_PRINCIPAL);
    callbackHandler.handle(new Callback[]{ac});

    // Expect immediate authorization when no LDAP filtering is enabled
    assertTrue(ac.isAuthorized());
    verifyNoInteractions(dirSearch);
  }

  @Test
  public void testAuthorizeWithUserGroupSearchFilterSuccess() throws Exception {
    // Enable group check and configure UserGroupSearchFilter
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);

    String userSearchFilter = "(&(uid={0})(objectClass=person))";
    String baseDn = "dc=example,dc=com";
    String groupSearchFilter = "(&(memberUid={0})(objectClass=posixGroup))";
    String groupBaseDn = "ou=groups,dc=example,dc=com";
    String userDn = "uid=user,dc=example,dc=com";

    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERSEARCHFILTER, userSearchFilter);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN, baseDn);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPSEARCHFILTER, groupSearchFilter);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPBASEDN, groupBaseDn);

    // Mock with specific values
    when(dirSearch.findUserDn(eq(TEST_USER), eq(userSearchFilter), eq(baseDn)))
        .thenReturn(userDn);
    when(dirSearch.executeUserAndGroupFilterQuery(
        eq(TEST_USER),
        eq(userDn),
        eq(groupSearchFilter),
        eq(groupBaseDn)))
        .thenReturn(Collections.singletonList("cn=group1,ou=groups,dc=example,dc=com"));

    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);

    AuthorizeCallback ac = new AuthorizeCallback(TEST_PRINCIPAL, TEST_PRINCIPAL);
    callbackHandler.handle(new Callback[]{ac});

    assertTrue(ac.isAuthorized());
    verify(dirSearchFactory).getInstance(eq(conf), eq("bindUser"), eq("bindPassword"));
    verify(dirSearch).findUserDn(eq(TEST_USER), eq(userSearchFilter), eq(baseDn));
    verify(dirSearch).executeUserAndGroupFilterQuery(
        eq(TEST_USER), eq(userDn), eq(groupSearchFilter), eq(groupBaseDn));
  }

  @Test
  public void testAuthorizeWithCustomQueryFilterSuccess() throws Exception {
    // Enable group check and configure CustomQueryFilter
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);

    String customQuery = "(&(objectClass=person)(uid=" + TEST_USER + "))";
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_CUSTOMLDAPQUERY, customQuery);

    // Mock with specific query
    when(dirSearch.executeCustomQuery(eq(customQuery)))
        .thenReturn(Collections.singletonList("uid=" + TEST_USER + ",dc=example,dc=com"));

    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);

    AuthorizeCallback ac = new AuthorizeCallback(TEST_PRINCIPAL, TEST_PRINCIPAL);
    callbackHandler.handle(new Callback[]{ac});

    assertTrue(ac.isAuthorized());
    verify(dirSearchFactory).getInstance(eq(conf), eq("bindUser"), eq("bindPassword"));
    verify(dirSearch).executeCustomQuery(eq(customQuery));
  }

  @Test
  public void testAuthorizeWithGroupFilterFailure() throws Exception {
    // Enable group check and configure GroupFilter
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "group1,group2");

    String userDn = "uid=user,dc=example,dc=com";
    String wrongGroupDn = "cn=group3,dc=example,dc=com";

    // Mock with specific DN values
    when(dirSearch.findUserDn(TEST_USER)).thenReturn(userDn);
    when(dirSearch.findGroupsForUser(eq(userDn)))
        .thenReturn(Collections.singletonList(wrongGroupDn));

    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);

    AuthorizeCallback ac = new AuthorizeCallback(TEST_PRINCIPAL, TEST_PRINCIPAL);
    callbackHandler.handle(new Callback[]{ac});

    assertFalse(ac.isAuthorized());
    verify(dirSearchFactory).getInstance(eq(conf), eq("bindUser"), eq("bindPassword"));
    verify(dirSearch, times(2)).findUserDn(TEST_USER);
    verify(dirSearch).findGroupsForUser(eq(userDn));
  }

  @Test
  public void testAuthorizeWithNoFilterConfigured() throws Exception {
    // Enable group check but don't configure any filters
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    // No filters configured - resolveFilter will return null

    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);

    AuthorizeCallback ac = new AuthorizeCallback(TEST_PRINCIPAL, TEST_PRINCIPAL);
    callbackHandler.handle(new Callback[]{ac});

    // Should authorize since no filter is configured (logged warning)
    assertTrue(ac.isAuthorized());
    verifyNoInteractions(dirSearch);
  }

  @Test
  public void testDelegationWithDifferentAuthIds() throws Exception {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);

    String authorizationId = "anotheruser@TEST.REALM";
    AuthorizeCallback ac = new AuthorizeCallback(TEST_PRINCIPAL, authorizationId);
    Callback[] callbacks = {ac};

    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);
    callbackHandler.handle(callbacks);

    // Since authentication and authorization IDs differ, the handler should delegate
    verify(delegateHandler).handle(argThat(callbackArray ->
        callbackArray.length == 1 && callbackArray[0] == ac));
    verifyNoInteractions(dirSearch);
  }

  @Test
  public void testAuthorizeWithMissingBindCredentials() throws Exception {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "group1");
    conf.unset(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BIND_USER.varname);

    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);

    AuthorizeCallback ac = new AuthorizeCallback(TEST_PRINCIPAL, TEST_PRINCIPAL);
    callbackHandler.handle(new Callback[]{ac});

    // Missing bind credentials should cause authorization failure
    assertFalse(ac.isAuthorized());
    verifyNoInteractions(dirSearch);
  }

  @Test(expected = UnsupportedCallbackException.class)
  public void testHandleUnsupportedCallback() throws Exception {
    Callback unsupportedCallback = mock(Callback.class);
    Callback[] callbacks = {unsupportedCallback};

    doThrow(new UnsupportedCallbackException(unsupportedCallback))
        .when(delegateHandler).handle(any(Callback[].class));

    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);

    callbackHandler.handle(callbacks);
  }

  @Test
  public void testHandleMixedCallbacks() throws Exception {
    // Enable group check and configure a filter
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LDAP_ENABLE_GROUP_CHECK_AFTER_KERBEROS, true);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERFILTER, TEST_USER);

    // When USERFILTER is set, UserSearchFilterFactory is activated which calls findUserDn
    when(dirSearch.findUserDn(TEST_USER)).thenReturn("uid=user,dc=example,dc=com");

    AuthorizeCallback ac = new AuthorizeCallback(TEST_PRINCIPAL, TEST_PRINCIPAL);
    Callback unsupportedCallback = mock(Callback.class);
    Callback[] callbacks = {ac, unsupportedCallback};

    doThrow(new UnsupportedCallbackException(unsupportedCallback))
        .when(delegateHandler).handle(any(Callback[].class));

    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);

    try {
      callbackHandler.handle(callbacks);
      fail("Expected UnsupportedCallbackException");
    } catch (UnsupportedCallbackException e) {
      assertEquals(unsupportedCallback, e.getCallback());
      // AuthorizeCallback should have been processed
      assertTrue(ac.isAuthorized());
      verify(dirSearchFactory).getInstance(eq(conf), eq("bindUser"), eq("bindPassword"));
      verify(dirSearch).findUserDn(TEST_USER);
    }
  }

  @Test
  public void testHandleDelegatesWhenAuthIdsMissing() throws Exception {
    callbackHandler = new LdapGroupCallbackHandler(conf, dirSearchFactory, delegateHandler);

    AuthorizeCallback ac = new AuthorizeCallback(null, TEST_PRINCIPAL);

    callbackHandler.handle(new Callback[]{ac});

    verify(delegateHandler).handle(any(Callback[].class));
    verifyNoInteractions(dirSearch);
    assertFalse(ac.isAuthorized());
  }
}
