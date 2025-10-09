/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.hive.metastore;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;

import org.apache.hadoop.hive.metastore.ldap.LdapAuthenticationTestCase;
import org.apache.hadoop.hive.metastore.ldap.User;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertTrue;


/**
 * TestSuite to test Metastore's LDAP Authentication provider with an
 * in-process LDAP Server (Apache Directory Server instance).
 *
 */
@RunWith(FrameworkRunner.class)
@CreateLdapServer(transports = {
  @CreateTransport(protocol = "LDAP"),
  @CreateTransport(protocol = "LDAPS")
})

@CreateDS(partitions = {
  @CreatePartition(
      name = "example",
      suffix = "dc=example,dc=com",
      contextEntry = @ContextEntry(entryLdif =
          "dn: dc=example,dc=com\n" +
          "dc: example\n" +
          "objectClass: top\n" +
          "objectClass: domain\n\n"
      ),
    indexes = {
      @CreateIndex(attribute = "objectClass"),
      @CreateIndex(attribute = "cn"),
      @CreateIndex(attribute = "uid")
    }
  )
})

@ApplyLdifFiles({
    "ldap/example.com.ldif",
    "ldap/microsoft.schema.ldif",
    "ldap/ad.example.com.ldif"
})

// standalone-metastore/metastore-server/src/test/resources/log4j2.properties sets root logger
// level to debug. All the logs are captured by CapturingLogAppender which creates an ArrayList
// out of those. The classes in org .apache.directory produce a lot of debug output. Hence
// running this test causes Java Heap OOM. If we change the logging level to "info" in log4j2
// .properties file, org.apache.hadoop.hive.metastore.metrics.TestMetrics fails since that relies
// on DEBUG level logging being captured. Even we if disable CapturingLogAppender, the debug
// output slows down the test and it takes a lot of time. So, for now disabling this test. This
// test is copy of org.apache.hive.service.auth.TestLdapAtnProviderWithMiniDS, which is passing.
// Once we deduplicate the LDAP code, this separate test will not be required. Till then we keep
// it here in case somebody wants to run it.
@Ignore
public class TestLdapAtnProviderWithMiniDS extends AbstractLdapTestUnit {

  private static final String GROUP1_NAME = "group1";
  private static final String GROUP2_NAME = "group2";
  private static final String GROUP3_NAME = "group3";
  private static final String GROUP4_NAME = "group4";

  private static final String GROUP_ADMINS_NAME = "admins";
  private static final String GROUP_TEAM1_NAME = "team1";
  private static final String GROUP_TEAM2_NAME = "team2";
  private static final String GROUP_RESOURCE1_NAME = "resource1";
  private static final String GROUP_RESOURCE2_NAME = "resource2";

  private static final User USER1 = User.builder()
      .id("user1")
      .useIdForPassword()
      .dn("uid=user1,ou=People,dc=example,dc=com")
      .build();

  private static final User USER2 = User.builder()
      .id("user2")
      .useIdForPassword()
      .dn("uid=user2,ou=People,dc=example,dc=com")
      .build();

  private static final User USER3 = User.builder()
      .id("user3")
      .useIdForPassword()
      .dn("cn=user3,ou=People,dc=example,dc=com")
      .build();

  private static final User USER4 = User.builder()
      .id("user4")
      .useIdForPassword()
      .dn("cn=user4,ou=People,dc=example,dc=com")
      .build();

  private static final User ENGINEER_1 = User.builder()
      .id("engineer1")
      .dn("sAMAccountName=engineer1,ou=Engineering,dc=ad,dc=example,dc=com")
      .password("engineer1-password")
      .build();

  private static final User ENGINEER_2 = User.builder()
      .id("engineer2")
      .dn("sAMAccountName=engineer2,ou=Engineering,dc=ad,dc=example,dc=com")
      .password("engineer2-password")
      .build();

  private static final User MANAGER_1 = User.builder()
      .id("manager1")
      .dn("sAMAccountName=manager1,ou=Management,dc=ad,dc=example,dc=com")
      .password("manager1-password")
      .build();

  private static final User MANAGER_2 = User.builder()
      .id("manager2")
      .dn("sAMAccountName=manager2,ou=Management,dc=ad,dc=example,dc=com")
      .password("manager2-password")
      .build();

  private static final User ADMIN_1 = User.builder()
      .id("admin1")
      .dn("sAMAccountName=admin1,ou=Administration,dc=ad,dc=example,dc=com")
      .password("admin1-password")
      .build();

  private LdapAuthenticationTestCase testCase;

  private LdapAuthenticationTestCase.Builder defaultBuilder() {
    return LdapAuthenticationTestCase.builder().ldapServer(ldapServer);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (ldapServer.isStarted()) {
      ldapServer.stop();
    }
  }

  @Test
  public void testLDAPServer() throws Exception {
    assertTrue(ldapServer.isStarted());
    assertTrue(ldapServer.getPort() > 0);
  }

  @Test
  public void testUserBindPositiveWithShortname() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithId());
  }

  @Test
  public void testUserBindPositiveWithShortnameOldConfig() {
    testCase = defaultBuilder()
        .baseDN("ou=People,dc=example,dc=com")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithId());
  }

  @Test
  public void testUserBindNegativeWithShortname() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .build();

    testCase.assertAuthenticateFailsUsingWrongPassword(USER1.credentialsWithId());
    testCase.assertAuthenticateFailsUsingWrongPassword(USER2.credentialsWithId());
  }

  @Test
  public void testUserBindNegativeWithShortnameOldConfig() {
    testCase = defaultBuilder()
        .baseDN("ou=People,dc=example,dc=com")
        .build();

    testCase.assertAuthenticateFailsUsingWrongPassword(USER1.credentialsWithId());
    testCase.assertAuthenticateFails(
        USER1.getDn(),
        USER2.getPassword());
    testCase.assertAuthenticateFailsUsingWrongPassword(USER2.credentialsWithId());
  }

  @Test
  public void testUserBindPositiveWithDN() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());
  }

  @Test
  public void testUserBindPositiveWithDNOldConfig() {
    testCase = defaultBuilder()
        .baseDN("ou=People,dc=example,dc=com")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());
  }

  @Test
  public void testUserBindPositiveWithDNWrongOldConfig() {
    testCase = defaultBuilder()
        .baseDN("ou=DummyPeople,dc=example,dc=com")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());
  }

  @Test
  public void testUserBindPositiveWithDNWrongConfig() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=DummyPeople,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=DummyGroups,dc=example,dc=com")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());
  }

  @Test
  public void testUserBindPositiveWithDNBlankConfig() {
    testCase = defaultBuilder()
        .userDNPatterns(" ")
        .groupDNPatterns(" ")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());
  }

  @Test
  public void testUserBindPositiveWithDNBlankOldConfig() throws Exception {
    testCase = defaultBuilder()
        .baseDN("")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());
  }

  @Test
  public void testUserBindNegativeWithDN() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .build();

    testCase.assertAuthenticateFailsUsingWrongPassword(USER1.credentialsWithDn());
    testCase.assertAuthenticateFails(
        USER1.getDn(),
        USER2.getPassword());
    testCase.assertAuthenticateFailsUsingWrongPassword(USER2.credentialsWithDn());
  }

  @Test
  public void testUserBindNegativeWithDNOldConfig() {
    testCase = defaultBuilder()
        .baseDN("ou=People,dc=example,dc=com")
        .build();

    testCase.assertAuthenticateFailsUsingWrongPassword(USER1.credentialsWithDn());
    testCase.assertAuthenticateFails(
        USER1.getDn(),
        USER2.getPassword());
    testCase.assertAuthenticateFailsUsingWrongPassword(USER2.credentialsWithDn());
  }

  @Test
  public void testUserFilterPositive() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .userFilters(USER1.getId())
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());

    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .userFilters(USER2.getId())
        .build();

    testCase.assertAuthenticatePasses(USER2.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());

    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .userFilters(
            USER1.getId(),
            USER2.getId())
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());
  }

  @Test
  public void testUserFilterNegative() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .userFilters(USER2.getId())
        .build();

    testCase.assertAuthenticateFails(USER1.credentialsWithId());
    testCase.assertAuthenticateFails(USER1.credentialsWithDn());

    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .userFilters(USER1.getId())
        .build();

    testCase.assertAuthenticateFails(USER2.credentialsWithId());
    testCase.assertAuthenticateFails(USER2.credentialsWithDn());

    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .userFilters(USER3.getId())
        .build();

    testCase.assertAuthenticateFails(USER1.credentialsWithId());
    testCase.assertAuthenticateFails(USER2.credentialsWithId());
  }

  @Test
  public void testGroupFilterPositive() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .groupFilters(
            GROUP1_NAME,
            GROUP2_NAME)
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());

    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .groupFilters(GROUP2_NAME)
        .build();

    testCase.assertAuthenticatePasses(USER2.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());
  }

  @Test
  public void testGroupFilterNegative() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .groupFilters(GROUP2_NAME)
        .build();


    testCase.assertAuthenticateFails(USER1.credentialsWithId());
    testCase.assertAuthenticateFails(USER1.credentialsWithDn());


    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .groupFilters(GROUP1_NAME)
        .build();

    testCase.assertAuthenticateFails(USER2.credentialsWithId());
    testCase.assertAuthenticateFails(USER2.credentialsWithDn());
  }

  @Test
  public void testUserAndGroupFilterPositive() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .userFilters(
            USER1.getId(),
            USER2.getId())
        .groupFilters(
            GROUP1_NAME,
            GROUP2_NAME)
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());
  }

  @Test
  public void testUserAndGroupFilterNegative() {
    testCase = defaultBuilder()
        .userDNPatterns("uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("uid=%s,ou=Groups,dc=example,dc=com")
        .userFilters(
            USER1.getId(),
            USER2.getId())
        .groupFilters(
            GROUP3_NAME,
            GROUP3_NAME)
        .build();

    testCase.assertAuthenticateFails(USER2.credentialsWithDn());
    testCase.assertAuthenticateFails(USER2.credentialsWithId());
    testCase.assertAuthenticateFails(USER3.credentialsWithDn());
    testCase.assertAuthenticateFails(USER3.credentialsWithId());
  }

  @Test
  public void testCustomQueryPositive() {
    testCase = defaultBuilder()
        .baseDN("ou=People,dc=example,dc=com")
        .userDNPatterns(
            "cn=%s,ou=People,dc=example,dc=com",
            "uid=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("cn=%s,ou=People,dc=example,dc=com")
        .customQuery(
            String.format("(&(objectClass=person)(|(uid=%s)(uid=%s)))",
                USER1.getId(),
                USER4.getId()))
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER4.credentialsWithId());
    testCase.assertAuthenticatePasses(USER4.credentialsWithDn());
  }

  @Test
  public void testCustomQueryNegative() {
    testCase = defaultBuilder()
        .baseDN("ou=People,dc=example,dc=com")
        .customQuery(
            String.format("(&(objectClass=person)(uid=%s))",
                USER1.getId()))
        .build();

    testCase.assertAuthenticateFails(USER2.credentialsWithDn());
    testCase.assertAuthenticateFails(USER2.credentialsWithId());
  }

  /**
   Test to test the LDAP Atn to use a custom LDAP query that returns
   a) A set of group DNs
   b) A combination of group(s) DN and user DN
   LDAP atn is expected to extract the members of the group using the attribute value for
   "hive.server2.authentication.ldap.groupMembershipKey"
   */
  @Test
  public void testCustomQueryWithGroupsPositive() {
    testCase = defaultBuilder()
        .baseDN("dc=example,dc=com")
        .userDNPatterns(
            "cn=%s,ou=People,dc=example,dc=com",
            "uid=%s,ou=People,dc=example,dc=com")
        .customQuery(
            String.format("(&(objectClass=groupOfNames)(|(cn=%s)(cn=%s)))",
                GROUP1_NAME,
                GROUP2_NAME))
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER2.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithDn());

    /* the following test uses a query that returns a group and a user entry.
       the ldap atn should use the groupMembershipKey to identify the users for the returned group
       and the authentication should succeed for the users of that group as well as the lone user4 in this case
    */
    testCase = defaultBuilder()
        .baseDN("dc=example,dc=com")
        .userDNPatterns(
            "cn=%s,ou=People,dc=example,dc=com",
            "uid=%s,ou=People,dc=example,dc=com")
        .customQuery(
            String.format("(|(&(objectClass=groupOfNames)(cn=%s))(&(objectClass=person)(sn=%s)))",
                GROUP1_NAME,
                USER4.getId()))
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER1.credentialsWithDn());
    testCase.assertAuthenticatePasses(USER4.credentialsWithId());
    testCase.assertAuthenticatePasses(USER4.credentialsWithDn());


    testCase = defaultBuilder()
        .baseDN("dc=example,dc=com")
        .userDNPatterns(
            "cn=%s,ou=People,dc=example,dc=com",
            "uid=%s,ou=People,dc=example,dc=com")
        .groupMembershipKey("uniqueMember")
        .customQuery(
            String.format("(&(objectClass=groupOfUniqueNames)(cn=%s))",
                GROUP4_NAME))
        .build();

    testCase.assertAuthenticatePasses(USER4.credentialsWithId());
    testCase.assertAuthenticatePasses(USER4.credentialsWithDn());
  }

  @Test
  public void testCustomQueryWithGroupsNegative() {
    testCase = defaultBuilder()
        .baseDN("dc=example,dc=com")
        .userDNPatterns(
            "cn=%s,ou=People,dc=example,dc=com",
            "uid=%s,ou=People,dc=example,dc=com")
        .customQuery(
            String.format("(&(objectClass=groupOfNames)(|(cn=%s)(cn=%s)))",
                GROUP1_NAME,
                GROUP2_NAME))
        .build();

    testCase.assertAuthenticateFails(USER3.credentialsWithDn());
    testCase.assertAuthenticateFails(USER3.credentialsWithId());
  }

  @Test
  public void testGroupFilterPositiveWithCustomGUID() {
    testCase = defaultBuilder()
        .userDNPatterns("cn=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("cn=%s,ou=Groups,dc=example,dc=com")
        .groupFilters(GROUP3_NAME)
        .guidKey("cn")
        .build();

    testCase.assertAuthenticatePasses(USER3.credentialsWithId());
    testCase.assertAuthenticatePasses(USER3.credentialsWithDn());
  }

  @Test
  public void testGroupFilterPositiveWithCustomAttributes() {
    testCase = defaultBuilder()
        .userDNPatterns("cn=%s,ou=People,dc=example,dc=com")
        .groupDNPatterns("cn=%s,ou=Groups,dc=example,dc=com")
        .groupFilters(GROUP4_NAME)
        .guidKey("cn")
        .groupMembershipKey("uniqueMember")
        .groupClassKey("groupOfUniqueNames")
        .build();

    testCase.assertAuthenticatePasses(USER4.credentialsWithId());
    testCase.assertAuthenticatePasses(USER4.credentialsWithDn());
  }

  @Test
  public void testDirectUserMembershipGroupFilterPositive() {
    testCase = defaultBuilder()
        .userDNPatterns(
            "sAMAccountName=%s,ou=Engineering,dc=ad,dc=example,dc=com",
            "sAMAccountName=%s,ou=Management,dc=ad,dc=example,dc=com")
        .groupDNPatterns(
            "sAMAccountName=%s,ou=Teams,dc=ad,dc=example,dc=com",
            "sAMAccountName=%s,ou=Resources,dc=ad,dc=example,dc=com")
        .groupFilters(
            GROUP_TEAM1_NAME,
            GROUP_TEAM2_NAME,
            GROUP_RESOURCE1_NAME,
            GROUP_RESOURCE2_NAME)
        .guidKey("sAMAccountName")
        .userMembershipKey("memberOf")
        .build();

    testCase.assertAuthenticatePasses(ENGINEER_1.credentialsWithId());
    testCase.assertAuthenticatePasses(ENGINEER_2.credentialsWithId());
    testCase.assertAuthenticatePasses(MANAGER_1.credentialsWithId());
    testCase.assertAuthenticatePasses(MANAGER_2.credentialsWithId());
  }

  @Test
  public void testDirectUserMembershipGroupFilterNegative() {
    testCase = defaultBuilder()
        .userDNPatterns(
            "sAMAccountName=%s,ou=Engineering,dc=ad,dc=example,dc=com",
            "sAMAccountName=%s,ou=Management,dc=ad,dc=example,dc=com")
        .groupDNPatterns("cn=%s,ou=Teams,dc=ad,dc=example,dc=com")
        .groupFilters(GROUP_TEAM1_NAME)
        .guidKey("sAMAccountName")
        .userMembershipKey("memberOf")
        .build();

    testCase.assertAuthenticateFails(ENGINEER_2.credentialsWithId());
    testCase.assertAuthenticateFails(MANAGER_2.credentialsWithId());
  }

  @Test
  public void testDirectUserMembershipGroupFilterNegativeWithoutUserBases() throws Exception {
    testCase = defaultBuilder()
        .groupDNPatterns("cn=%s,ou=Teams,dc=ad,dc=example,dc=com")
        .groupFilters(GROUP_TEAM1_NAME)
        .guidKey("sAMAccountName")
        .userMembershipKey("memberOf")
        .build();

    testCase.assertAuthenticateFails(ENGINEER_1.credentialsWithId());
    testCase.assertAuthenticateFails(ENGINEER_2.credentialsWithId());
    testCase.assertAuthenticateFails(MANAGER_1.credentialsWithId());
    testCase.assertAuthenticateFails(MANAGER_2.credentialsWithId());
  }

  @Test
  public void testDirectUserMembershipGroupFilterWithDNCredentials() throws Exception {
    testCase = defaultBuilder()
        .userDNPatterns("sAMAccountName=%s,ou=Engineering,dc=ad,dc=example,dc=com")
        .groupDNPatterns("cn=%s,ou=Teams,dc=ad,dc=example,dc=com")
        .groupFilters(GROUP_TEAM1_NAME)
        .guidKey("sAMAccountName")
        .userMembershipKey("memberOf")
        .build();

    testCase.assertAuthenticatePasses(ENGINEER_1.credentialsWithDn());
    testCase.assertAuthenticateFails(MANAGER_1.credentialsWithDn());
  }

  @Test
  public void testDirectUserMembershipGroupFilterWithDifferentGroupClassKey() throws Exception {
    testCase = defaultBuilder()
        .userDNPatterns("sAMAccountName=%s,ou=Administration,dc=ad,dc=example,dc=com")
        .groupDNPatterns("cn=%s,ou=Administration,dc=ad,dc=example,dc=com")
        .groupFilters(GROUP_ADMINS_NAME)
        .guidKey("sAMAccountName")
        .userMembershipKey("memberOf")
        .groupClassKey("groupOfUniqueNames")
        .build();

    testCase.assertAuthenticatePasses(ADMIN_1.credentialsWithId());
    testCase.assertAuthenticateFails(ENGINEER_1.credentialsWithId());
    testCase.assertAuthenticateFails(MANAGER_1.credentialsWithDn());
  }

  @Test
  public void testDirectUserMembershipGroupFilterNegativeWithWrongGroupClassKey() throws Exception {
    testCase = defaultBuilder()
        .userDNPatterns("sAMAccountName=%s,ou=Administration,dc=ad,dc=example,dc=com")
        .groupDNPatterns("cn=%s,ou=Administration,dc=ad,dc=example,dc=com")
        .groupFilters(GROUP_ADMINS_NAME)
        .guidKey("sAMAccountName")
        .userMembershipKey("memberOf")
        .groupClassKey("wrongClass")
        .build();

    testCase.assertAuthenticateFails(ADMIN_1.credentialsWithId());
    testCase.assertAuthenticateFails(ENGINEER_1.credentialsWithId());
    testCase.assertAuthenticateFails(MANAGER_1.credentialsWithDn());
  }

  @Test
  public void testUserSearchFilterPositive() throws Exception {
    testCase = defaultBuilder()
        .baseDN("ou=People,dc=example,dc=com")
        .userSearchFilter("(&(|(uid={0})(sAMAccountName={0}))(objectClass=person))")
        .guidKey("uid")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithId());

    testCase = defaultBuilder()
        .baseDN("ou=Engineering,dc=ad,dc=example,dc=com")
        .userSearchFilter("(&(|(uid={0})(sAMAccountName={0}))(objectClass=person))")
        .guidKey("sAMAccountName")
        .build();

    testCase.assertAuthenticatePasses(ENGINEER_1.credentialsWithId());
    testCase.assertAuthenticatePasses(ENGINEER_1.credentialsWithDn());
  }

  @Test
  public void testUserSearchFilterNegative() throws Exception {
    testCase = defaultBuilder()
        .baseDN("ou=Engineering,dc=ad,dc=example,dc=com")
        .userSearchFilter("(&(sAMAccountName={0})(objectClass=person)")
        .guidKey("uid")
        .build();

    testCase.assertAuthenticateFails(USER1.credentialsWithId());
    testCase.assertAuthenticateFails(USER3.credentialsWithId());
  }

  @Test
  public void testUserAndGroupSearchFilterPositive() throws Exception {
    testCase = defaultBuilder()
        .baseDN("ou=People,dc=example,dc=com")
        .userSearchFilter("(&(|(uid={0})(sAMAccountName={0}))(objectClass=person))")
        .guidKey("uid")
        .groupBaseDN("ou=Groups,dc=example,dc=com")
        .groupSearchFilter("(&(|(member={0})(member={1}))(&(|(cn=group1)(cn=group2))(objectClass=groupOfNames)))")
        .build();

    testCase.assertAuthenticatePasses(USER1.credentialsWithId());
    testCase.assertAuthenticatePasses(USER2.credentialsWithId());
  }

  @Test
  public void testUserAndGroupSearchFilterNegative() throws Exception {
    testCase = defaultBuilder()
        .baseDN("ou=People,dc=example,dc=com")
        .userSearchFilter("(&(|(uid={0})(sAMAccountName={0}))(objectClass=person)")
        .guidKey("uid")
        .groupBaseDN("ou=Groups,dc=example,dc=com")
        .groupSearchFilter("(&(|(member={0})(member={1}))(&(cn=group1)(objectClass=groupOfNames)))")
        .build();

    testCase.assertAuthenticateFails(USER2.credentialsWithId());
    testCase.assertAuthenticateFails(USER3.credentialsWithId());
    testCase.assertAuthenticateFails(ENGINEER_1.credentialsWithId());
  }

}
