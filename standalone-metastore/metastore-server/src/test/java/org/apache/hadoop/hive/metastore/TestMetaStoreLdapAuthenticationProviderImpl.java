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
package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.util.Arrays;
import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.hive.metastore.ldap.DirSearch;
import org.apache.hadoop.hive.metastore.ldap.DirSearchFactory;
import org.apache.hadoop.hive.metastore.ldap.LdapSearchFactory;
import org.junit.Test;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestMetaStoreLdapAuthenticationProviderImpl {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  public Configuration conf;
  public MetaStoreLdapAuthenticationProviderImpl auth;

  @Mock
  public DirSearchFactory factory;

  @Mock
  public DirSearch search;

  @Before
  public void setup() throws AuthenticationException {
    conf = MetastoreConf.newMetastoreConf();
    conf.set("hive.root.logger", "DEBUG,console");
    conf.set("hive.server2.authentication.ldap.url", "localhost");
    when(factory.getInstance(any(Configuration.class), anyString(), anyString())).thenReturn(search);
  }

  @Test
  public void authenticateGivenBlankPassword() throws Exception {
    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, new LdapSearchFactory());
    expectAuthenticationExceptionForInvalidPassword();
    auth.authenticate("user", "");
  }

  @Test
  public void authenticateGivenStringWithNullCharacterForPassword() throws Exception {
    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, new LdapSearchFactory());
    expectAuthenticationExceptionForInvalidPassword();
    auth.authenticate("user", "\0");
  }

  @Test
  public void authenticateGivenNullForPassword() throws Exception {
    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, new LdapSearchFactory());
    expectAuthenticationExceptionForInvalidPassword();
    auth.authenticate("user", null);
  }

  @Test
  public void testAuthenticateNoUserOrGroupFilter() throws NamingException, AuthenticationException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERDNPATTERN,
        "cn=%s,ou=Users,dc=mycorp,dc=com:cn=%s,ou=PowerUsers,dc=mycorp,dc=com");

    DirSearchFactory factory = mock(DirSearchFactory.class);

    lenient().when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");

    when(factory.getInstance(conf, "cn=user1,ou=PowerUsers,dc=mycorp,dc=com", "Blah")).thenReturn(search);
    when(factory.getInstance(conf, "cn=user1,ou=Users,dc=mycorp,dc=com", "Blah")).thenThrow(AuthenticationException.class);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate("user1", "Blah");

    verify(factory, times(2)).getInstance(isA(Configuration.class), anyString(), eq("Blah"));
    verify(search, atLeastOnce()).close();
  }

  @Test
  public void testAuthenticateWhenUserFilterPasses() throws NamingException, AuthenticationException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER,
        "user1,user2");

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");
    when(search.findUserDn("user2")).thenReturn("cn=user2,ou=PowerUsers,dc=mycorp,dc=com");

    authenticateUserAndCheckSearchIsClosed("user1");
    authenticateUserAndCheckSearchIsClosed("user2");
  }

  @Test
  public void testAuthenticateWhenLoginWithDomainAndUserFilterPasses() throws NamingException, AuthenticationException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER,
        "user1");

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");

    authenticateUserAndCheckSearchIsClosed("user1@mydomain.com");
  }

  @Test
  public void testAuthenticateWhenLoginWithDnAndUserFilterPasses() throws NamingException, AuthenticationException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER,
        "user1");

    when(search.findUserDn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");

    authenticateUserAndCheckSearchIsClosed("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");
  }

  @Test
  public void testAuthenticateWhenUserSearchFails() throws NamingException, AuthenticationException, IOException {
    thrown.expect(AuthenticationException.class);

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "user1,user2");

    when(search.findUserDn("user1")).thenReturn(null);

    authenticateUserAndCheckSearchIsClosed("user1");
  }

  @Test
  public void testAuthenticateWhenUserFilterFails() throws NamingException, AuthenticationException, IOException {
    thrown.expect(AuthenticationException.class);

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "user1,user2");

    when(search.findUserDn("user3")).thenReturn("cn=user3,ou=PowerUsers,dc=mycorp,dc=com");

    authenticateUserAndCheckSearchIsClosed("user3");
  }

  @Test
  public void testAuthenticateWhenGroupMembershipKeyFilterPasses() throws NamingException, AuthenticationException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPFILTER, "group1,group2");

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");
    when(search.findUserDn("user2")).thenReturn("cn=user2,ou=PowerUsers,dc=mycorp,dc=com");

    when(search.findGroupsForUser("cn=user1,ou=PowerUsers,dc=mycorp,dc=com"))
        .thenReturn(Arrays.asList(
            "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
            "cn=group1,ou=Groups,dc=mycorp,dc=com"));
    when(search.findGroupsForUser("cn=user2,ou=PowerUsers,dc=mycorp,dc=com"))
        .thenReturn(Arrays.asList(
            "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
            "cn=group2,ou=Groups,dc=mycorp,dc=com"));

    authenticateUserAndCheckSearchIsClosed("user1");
    authenticateUserAndCheckSearchIsClosed("user2");
  }

  @Test
  public void testAuthenticateWhenUserAndGroupMembershipKeyFiltersPass() throws NamingException, AuthenticationException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPFILTER, "group1,group2");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "user1,user2");

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");
    when(search.findUserDn("user2")).thenReturn("cn=user2,ou=PowerUsers,dc=mycorp,dc=com");

    when(search.findGroupsForUser("cn=user1,ou=PowerUsers,dc=mycorp,dc=com"))
        .thenReturn(Arrays.asList(
            "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
            "cn=group1,ou=Groups,dc=mycorp,dc=com"));
    when(search.findGroupsForUser("cn=user2,ou=PowerUsers,dc=mycorp,dc=com"))
        .thenReturn(Arrays.asList(
            "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
            "cn=group2,ou=Groups,dc=mycorp,dc=com"));

    authenticateUserAndCheckSearchIsClosed("user1");
    authenticateUserAndCheckSearchIsClosed("user2");
  }

  @Test
  public void testAuthenticateWhenUserFilterPassesAndGroupMembershipKeyFilterFails()
      throws NamingException, AuthenticationException, IOException {
    thrown.expect(AuthenticationException.class);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPFILTER, "group1,group2");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "user1,user2");

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");

    when(search.findGroupsForUser("cn=user1,ou=PowerUsers,dc=mycorp,dc=com"))
        .thenReturn(Arrays.asList(
            "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
            "cn=OtherGroup,ou=Groups,dc=mycorp,dc=com"));

    authenticateUserAndCheckSearchIsClosed("user1");
  }

  @Test
  public void testAuthenticateWhenUserFilterFailsAndGroupMembershipKeyFilterPasses()
      throws NamingException, AuthenticationException, IOException {
    thrown.expect(AuthenticationException.class);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPFILTER, "group3");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "user1,user2");

    when(search.findUserDn("user3")).thenReturn("cn=user3,ou=PowerUsers,dc=mycorp,dc=com");

    lenient().when(search.findGroupsForUser("cn=user3,ou=PowerUsers,dc=mycorp,dc=com"))
        .thenReturn(Arrays.asList(
            "cn=testGroup,ou=Groups,dc=mycorp,dc=com",
            "cn=group3,ou=Groups,dc=mycorp,dc=com"));

    authenticateUserAndCheckSearchIsClosed("user3");
  }

  @Test
  public void testAuthenticateWhenCustomQueryFilterPasses() throws NamingException, AuthenticationException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BASEDN, "dc=mycorp,dc=com");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_CUSTOMLDAPQUERY,
        "(&(objectClass=person)(|(memberOf=CN=Domain Admins,CN=Users,DC=apache,DC=org)(memberOf=CN=Administrators,CN=Builtin,DC=apache,DC=org)))");

    when(search.executeCustomQuery(anyString())).thenReturn(Arrays.asList(
        "cn=user1,ou=PowerUsers,dc=mycorp,dc=com",
        "cn=user2,ou=PowerUsers,dc=mycorp,dc=com"));

    authenticateUserAndCheckSearchIsClosed("user1");
  }

  @Test
  public void testAuthenticateWhenCustomQueryFilterFailsAndUserFilterPasses() throws NamingException, AuthenticationException, IOException {
    thrown.expect(AuthenticationException.class);

    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BASEDN, "dc=mycorp,dc=com");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_CUSTOMLDAPQUERY,
        "(&(objectClass=person)(|(memberOf=CN=Domain Admins,CN=Users,DC=apache,DC=org)(memberOf=CN=Administrators,CN=Builtin,DC=apache,DC=org)))");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "user3");

    lenient().when(search.findUserDn("user3")).thenReturn("cn=user3,ou=PowerUsers,dc=mycorp,dc=com");
    when(search.executeCustomQuery(anyString())).thenReturn(Arrays.asList(
        "cn=user1,ou=PowerUsers,dc=mycorp,dc=com",
        "cn=user2,ou=PowerUsers,dc=mycorp,dc=com"));

    authenticateUserAndCheckSearchIsClosed("user3");
  }

  @Test
  public void testAuthenticateWhenUserMembershipKeyFilterPasses() throws NamingException, AuthenticationException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPFILTER, "HIVE-USERS");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BASEDN, "dc=mycorp,dc=com");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERMEMBERSHIP_KEY, "memberOf");

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");

    String groupDn = "cn=HIVE-USERS,ou=Groups,dc=mycorp,dc=com";
    when(search.findGroupDn("HIVE-USERS")).thenReturn(groupDn);
    when(search.isUserMemberOfGroup("user1", groupDn)).thenReturn(true);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate("user1", "Blah");

    verify(factory, times(1)).getInstance(isA(Configuration.class), anyString(), eq("Blah"));
    verify(search, times(1)).findGroupDn(anyString());
    verify(search, times(1)).isUserMemberOfGroup(anyString(), anyString());
    verify(search, atLeastOnce()).close();
  }

  @Test
  public void testAuthenticateWhenUserMembershipKeyFilterFails() throws NamingException, AuthenticationException, IOException {
    thrown.expect(AuthenticationException.class);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPFILTER, "HIVE-USERS");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BASEDN, "dc=mycorp,dc=com");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERMEMBERSHIP_KEY, "memberOf");

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");

    String groupDn = "cn=HIVE-USERS,ou=Groups,dc=mycorp,dc=com";
    when(search.findGroupDn("HIVE-USERS")).thenReturn(groupDn);
    when(search.isUserMemberOfGroup("user1", groupDn)).thenReturn(false);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate("user1", "Blah");
  }

  @Test
  public void testAuthenticateWhenUserMembershipKeyFilter2x2PatternsPasses() throws NamingException, AuthenticationException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPFILTER, "HIVE-USERS1,HIVE-USERS2");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPDNPATTERN,
        "cn=%s,ou=Groups,ou=branch1,dc=mycorp,dc=com");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERDNPATTERN,
        "cn=%s,ou=Userss,ou=branch1,dc=mycorp,dc=com");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERMEMBERSHIP_KEY, "memberOf");

    when(search.findUserDn("user1")).thenReturn("cn=user1,ou=PowerUsers,dc=mycorp,dc=com");

    when(search.findGroupDn("HIVE-USERS1"))
        .thenReturn("cn=HIVE-USERS1,ou=Groups,ou=branch1,dc=mycorp,dc=com");
    when(search.findGroupDn("HIVE-USERS2"))
        .thenReturn("cn=HIVE-USERS2,ou=Groups,ou=branch1,dc=mycorp,dc=com");

    when(search.isUserMemberOfGroup("user1", "cn=HIVE-USERS1,ou=Groups,ou=branch1,dc=mycorp,dc=com")).thenThrow(NamingException.class);
    when(search.isUserMemberOfGroup("user1", "cn=HIVE-USERS2,ou=Groups,ou=branch1,dc=mycorp,dc=com")).thenReturn(true);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate("user1", "Blah");

    verify(factory, times(1)).getInstance(isA(Configuration.class), anyString(), eq("Blah"));
    verify(search, times(2)).findGroupDn(anyString());
    verify(search, times(2)).isUserMemberOfGroup(anyString(), anyString());
    verify(search, atLeastOnce()).close();
  }

  @Test
  public void testAuthenticateWithBindInCredentialFilePasses() throws AuthenticationException, NamingException {
    String bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String bindPass = "testPassword";
    String authFullUser = "cn=user1,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String authUser = "user1";
    String authPass = "Blah";
    String tmpDir = System.getProperty("build.dir");
    String credentialsPath = "jceks://file" + tmpDir + "/test-classes/creds/test.jceks";
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_USER, bindUser);
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credentialsPath);

    System.out.println(tmpDir);

    when(search.findUserDn(eq(authUser))).thenReturn(authFullUser);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate(authUser, authPass);

    verify(factory, times(1)).getInstance(isA(Configuration.class), eq(bindUser), eq(bindPass));
    verify(factory, times(1)).getInstance(isA(Configuration.class), eq(authFullUser), eq(authPass));
    verify(search, times(1)).findUserDn(eq(authUser));
  }

  @Test
  public void testAuthenticateWithBindInMissingCredentialFilePasses() throws AuthenticationException, NamingException {
    String bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String authUser = "user1";
    String authPass = "Blah";
    String tmpDir = System.getProperty("build.dir");
    String credentialsPath = "jceks://file" + tmpDir + "/test-classes/creds/nonExistent.jceks";
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_USER, bindUser);
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, credentialsPath);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate(authUser, authPass);

    verify(factory, times(1)).getInstance(isA(Configuration.class), eq(authUser), eq(authPass));
  }

  @Test
  public void testAuthenticateWithBindUserPasses() throws AuthenticationException, NamingException {
    String bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String bindPass = "Blah";
    String authFullUser = "cn=user1,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String authUser = "user1";
    String authPass = "Blah2";
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_USER, bindUser);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_PASSWORD, bindPass);

    when(search.findUserDn(eq(authUser))).thenReturn(authFullUser);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate(authUser, authPass);

    verify(factory, times(1)).getInstance(isA(Configuration.class), eq(bindUser), eq(bindPass));
    verify(factory, times(1)).getInstance(isA(Configuration.class), eq(authFullUser), eq(authPass));
    verify(search, times(1)).findUserDn(eq(authUser));
  }

  @Test
  public void testAuthenticateWithBindUserFailsOnAuthentication() throws AuthenticationException, NamingException {
    String bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String bindPass = "Blah";
    String authFullUser = "cn=user1,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String authUser = "user1";
    String authPass = "Blah2";
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_USER, bindUser);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_PASSWORD, bindPass);

    thrown.expect(AuthenticationException.class);
    when(factory.getInstance(any(Configuration.class), eq(authFullUser), eq(authPass))).
      thenThrow(AuthenticationException.class);
    when(search.findUserDn(eq(authUser))).thenReturn(authFullUser);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate(authUser, authPass);
  }

  @Test
  public void testAuthenticateWithBindUserFailsOnGettingDn() throws AuthenticationException, NamingException {
    String bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String bindPass = "Blah";
    String authFullUser = "cn=user1,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String authUser = "user1";
    String authPass = "Blah2";
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_USER, bindUser);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_PASSWORD, bindPass);

    thrown.expect(AuthenticationException.class);
    when(search.findUserDn(eq(authUser))).thenThrow(NamingException.class);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate(authUser, authPass);
  }

  @Test
  public void testAuthenticateWithBindUserFailsOnBinding() throws AuthenticationException {
    String bindUser = "cn=BindUser,ou=Users,ou=branch1,dc=mycorp,dc=com";
    String bindPass = "Blah";
    String authUser = "user1";
    String authPass = "Blah2";
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_USER, bindUser);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BIND_PASSWORD, bindPass);

    thrown.expect(AuthenticationException.class);
    when(factory.getInstance(any(Configuration.class), eq(bindUser), eq(bindPass))).thenThrow(AuthenticationException.class);

    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    auth.authenticate(authUser, authPass);
  }

  private void expectAuthenticationExceptionForInvalidPassword() {
    thrown.expect(AuthenticationException.class);
    thrown.expectMessage("a null or blank password has been provided");
  }

  private void authenticateUserAndCheckSearchIsClosed(String user) throws IOException {
    auth = new MetaStoreLdapAuthenticationProviderImpl(conf, factory);
    try {
      auth.authenticate(user, "password doesn't matter");
    } finally {
      verify(search, atLeastOnce()).close();
    }
  }
}
