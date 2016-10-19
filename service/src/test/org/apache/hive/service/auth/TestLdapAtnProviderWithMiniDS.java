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

package org.apache.hive.service.auth;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;

import org.apache.hive.service.auth.ldap.LdapAuthenticationTestCase;
import org.apache.hive.service.auth.ldap.User;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertTrue;


/**
 * TestSuite to test Hive's LDAP Authentication provider with an
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

@ApplyLdifFiles("ldap/example.com.ldif")
public class TestLdapAtnProviderWithMiniDS extends AbstractLdapTestUnit {

  private static final String GROUP1_NAME = "group1";
  private static final String GROUP2_NAME = "group2";
  private static final String GROUP3_NAME = "group3";
  private static final String GROUP4_NAME = "group4";

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
        .groupMembership("uniqueMember")
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
        .groupMembership("uniqueMember")
        .groupClassKey("groupOfUniqueNames")
        .build();

    testCase.assertAuthenticatePasses(USER4.credentialsWithId());
    testCase.assertAuthenticatePasses(USER4.credentialsWithDn());
  }
}
