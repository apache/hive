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

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import javax.security.sasl.AuthenticationException;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.ContextEntry;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreateIndex;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;

import org.apache.hadoop.hive.conf.HiveConf;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;


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

  private static LdapAuthenticationProviderImpl ldapProvider;
  Map<String, String> ldapProperties;

  static final User USER1 = new User("user1", "user1", "uid=user1,ou=People,dc=example,dc=com");
  static final User USER2 = new User("user2", "user2", "uid=user2,ou=People,dc=example,dc=com");
  static final User USER3 = new User("user3", "user3", "cn=user3,ou=People,dc=example,dc=com");
  static final User USER4 = new User("user4", "user4", "cn=user4,ou=People,dc=example,dc=com");

  @Before
  public void setup() throws Exception {
    ldapProperties = new HashMap<>();
  }

  @BeforeClass
  public static void init() throws Exception {
    ldapProvider = new LdapAuthenticationProviderImpl(new HiveConf());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (ldapServer.isStarted()) {
      ldapServer.stop();
    }
  }

  private void initLdapAtn() throws Exception {
    String ldapUrl = "ldap://localhost:" + ldapServer.getPort();

    HiveConf hiveConf = new HiveConf();
    hiveConf.set("hive.root.logger", "DEBUG,console");
    hiveConf.set("hive.server2.authentication.ldap.url", ldapUrl);

    for (Map.Entry<String, String> entry : ldapProperties.entrySet()) {
      hiveConf.set(entry.getKey(), entry.getValue());
    }

    ldapProvider = new LdapAuthenticationProviderImpl(hiveConf);
  }

  @Test
  public void testLDAPServer() throws Exception {
    initLdapAtn();
    assertTrue(ldapServer.isStarted());
    assertTrue(ldapServer.getPort() > 0);
  }

  @Test
  public void testUserBindPositiveWithShortname() throws Exception {
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    initLdapAtn();
    String user;

    user = USER1.getUID();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user + " with password "
                  + USER1.getPassword() + ", expected to succeed");
    }

    user = USER2.getUID();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + USER2.getUID() + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user + " with password "
                  + USER2.getPassword() + ", expected to succeed");
    }
  }

  @Test
  public void testUserBindPositiveWithShortnameOldConfig() throws Exception {
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "ou=People,dc=example,dc=com");
    initLdapAtn();
    String user;

    user = USER1.getUID();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user + " with password "
                  + USER1.getPassword() + ", expected to succeed");
    }

    user = USER2.getUID();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + USER2.getUID() + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user + " with password "
                  + USER2.getPassword() + ", expected to succeed");
    }
  }

  @Test
  public void testUserBindNegativeWithShortname() throws Exception {
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    initLdapAtn();

    try {
      ldapProvider.Authenticate(USER1.getUID(), USER2.getPassword());
      Assert.fail("testUserBindNegative: Authentication succeeded for " + USER1.getUID() + " with password "
                  + USER2.getPassword() + ", expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for " + USER1.getUID() + " as expected", true);
    }

    try {
      ldapProvider.Authenticate(USER2.getUID(), "user");
      Assert.fail("testUserBindNegative: Authentication failed for " + USER2.getUID() + " with password user, expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for " + USER2.getUID() + " as expected", true);
    }
  }

  @Test
  public void testUserBindNegativeWithShortnameOldConfig() throws Exception {
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "ou=People,dc=example,dc=com");
    initLdapAtn();

    try {
      ldapProvider.Authenticate(USER1.getUID(), USER2.getPassword());
      Assert.fail("testUserBindNegative: Authentication succeeded for " + USER1.getUID() + " with password "
                  + USER2.getPassword() + ", expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for " + USER1.getUID() + " as expected", true);
    }

    try {
      ldapProvider.Authenticate(USER2.getUID(), "user");
      Assert.fail("testUserBindNegative: Authentication failed for " + USER2.getUID() + " with password user, expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for " + USER2.getUID() + " as expected", true);
    }
  }

  @Test
  public void testUserBindPositiveWithDN() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    initLdapAtn();
    assertTrue(ldapServer.getPort() > 0);

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER1.getPassword() + ", expected to succeed:" + e.getMessage());
    }

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " user as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER2.getPassword() + ", expected to succeed:" + e.getMessage());
    }
  }

  @Test
  public void testUserBindPositiveWithDNOldConfig() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "ou=People,dc=example,dc=com");
    initLdapAtn();
    assertTrue(ldapServer.getPort() > 0);

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER1.getPassword() + ", expected to succeed");
    }

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER2.getPassword() + ", expected to succeed");
    }
  }

  @Test
  public void testUserBindPositiveWithDNWrongOldConfig() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "ou=DummyPeople,dc=example,dc=com");
    initLdapAtn();
    assertTrue(ldapServer.getPort() > 0);

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER1.getPassword() + ", expected to succeed");
    }

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password "
                  + USER2.getPassword() + ", expected to succeed");
    }
  }

  @Test
  public void testUserBindPositiveWithDNWrongConfig() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=DummyPeople,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=DummyGroups,dc=example,dc=com");
    initLdapAtn();
    assertTrue(ldapServer.getPort() > 0);

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER1.getPassword() + ", expected to succeed");
    }

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER2.getPassword() + ", expected to succeed");
    }
  }

  @Test
  public void testUserBindPositiveWithDNBlankConfig() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", " ");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", " ");
    initLdapAtn();
    assertTrue(ldapServer.getPort() > 0);

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER1.getPassword() + ", expected to succeed");
    }

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER2.getPassword() + ", expected to succeed");
    }
  }

  @Test
  public void testUserBindPositiveWithDNBlankOldConfig() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "");
    initLdapAtn();
    assertTrue(ldapServer.getPort() > 0);

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER1.getPassword() + ", expected to succeed");
    }

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserBindPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password " + USER2.getPassword() + ", expected to succeed");
    }
  }

  @Test
  public void testUserBindNegativeWithDN() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    initLdapAtn();
    assertTrue(ldapServer.getPort() > 0);

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testUserBindNegative: Authentication succeeded for " + user + " with password " +
                   USER2.getPassword() + ", expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for " + user + " as expected", true);
    }

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, "user");
      Assert.fail("testUserBindNegative: Authentication failed for " + user + " with password user, " +
                    "expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for " + user + " as expected", true);
    }
  }

  @Test
  public void testUserBindNegativeWithDNOldConfig() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "ou=People,dc=example,dc=com");
    initLdapAtn();
    assertTrue(ldapServer.getPort() > 0);

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testUserBindNegative: Authentication succeeded for " + user + " with password " +
                   USER2.getPassword() + ", expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for " + user + " as expected", true);
    }

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, "user");
      Assert.fail("testUserBindNegative: Authentication failed for " + user + " with password user, " +
                    "expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for " + user + " as expected", true);
    }
  }

  @Test
  public void testUserFilterPositive() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userFilter", USER2.getUID());
    initLdapAtn();

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserFilterPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER2.getUID();
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserFilterPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserFilterPositive: Authentication failed for " + user + ",user expected to pass userfilter");
    }

    ldapProperties = new HashMap<>();
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userFilter", USER1.getUID());
    initLdapAtn();

    try {
      user = USER1.getDN();
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserFilterPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER1.getUID();
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserFilterPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserFilterPositive: Authentication failed for " + user + ",user expected to pass userfilter");
    }

    ldapProperties = new HashMap<>();
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userFilter", USER2.getUID() + "," + USER1.getUID());
    initLdapAtn();

    try {
      user = USER1.getDN();
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserFilterPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER2.getUID();
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserFilterPositive: Authentication succeeded for " + user + " as expected", true);

    } catch (AuthenticationException e) {
      Assert.fail("testUserFilterPositive: Authentication failed for user, user is expected to pass userfilter");
    }
  }

  @Test
  public void testUserFilterNegative() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userFilter", USER2.getUID());
    initLdapAtn();

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      Assert.fail("testUserFilterNegative: Authentication succeeded for " + user + ",user is expected to fail userfilter");
    } catch (AuthenticationException e) {
      assertTrue("testUserFilterNegative: Authentication failed for " + user + " as expected", true);
    }

    user = USER1.getUID();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      Assert.fail("testUserFilterNegative: Authentication succeeded for " + user + ",user is expected to fail userfilter");
    } catch (AuthenticationException e) {
      assertTrue("testUserFilterNegative: Authentication failed for " + user + " as expected", true);
    }

    ldapProperties = new HashMap<>();
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userFilter", USER1.getUID());
    initLdapAtn();

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testUserFilterNegative: Authentication succeeded for " + user + ",user is expected to fail userfilter");
    } catch (AuthenticationException e) {
      assertTrue("testUserFilterNegative: Authentication failed for " + user + " as expected", true);
    }

    user = USER2.getUID();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testUserFilterNegative: Authentication succeeded for " + user + ",user is expected to fail userfilter");
    } catch (AuthenticationException e) {
      assertTrue("testUserFilterNegative: Authentication failed for " + user + " as expected", true);
    }

    ldapProperties = new HashMap<>();
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userFilter", USER3.getUID());
    initLdapAtn();

    user = USER1.getUID();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      Assert.fail("testUserFilterNegative: Authentication succeeded for " + user + ",user expected to fail userfilter");
    } catch (AuthenticationException e) {
      assertTrue("testUserFilterNegative: Authentication failed for " + user + " as expected", true);
    }

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testUserFilterNegative: Authentication succeeded for " + user + ",user expected to fail userfilter");
    } catch (AuthenticationException e) {
      assertTrue("testUserFilterNegative: Authentication failed for " + user + " as expected", true);
    }
  }

  @Test
  public void testGroupFilterPositive() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupFilter", "group1,group2");
    initLdapAtn();

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER1.getUID();
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER2.getDN();
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testGroupFilterPositive: Authentication failed for " + user + ",user expected to pass groupfilter");
    }

    ldapProperties = new HashMap<>();
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupFilter", "group2");
    initLdapAtn();

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testGroupFilterPositive: Authentication failed for " + user + ",user expected to pass groupfilter");
    }
  }

  @Test
  public void testGroupFilterNegative() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupFilter", "group1");
    initLdapAtn();

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testGroupFilterNegative: Authentication succeeded for " + user + ",user expected to fail groupfilter");
    } catch (AuthenticationException e) {
      assertTrue("testGroupFilterNegative: Authentication failed for " + user + " as expected", true);
    }

    ldapProperties = new HashMap<>();
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupFilter", "group2");
    initLdapAtn();

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      Assert.fail("testGroupFilterNegative: Authentication succeeded for " + user + ",user expected to fail groupfilter");
    } catch (AuthenticationException e) {
      assertTrue("testGroupFilterNegative: Authentication failed for " + user + " as expected", true);
    }
  }

  @Test
  public void testUserAndGroupFilterPositive() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userFilter", USER1.getUID() + "," + USER2.getUID());
    ldapProperties.put("hive.server2.authentication.ldap.groupFilter", "group1,group2");
    initLdapAtn();

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserAndGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER1.getUID();
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testUserAndGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);

    } catch (AuthenticationException e) {
      Assert.fail("testUserAndGroupFilterPositive: Authentication failed for " + user + ",user expected to pass groupfilter");
    }

    user = USER2.getUID();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      assertTrue("testUserAndGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserAndGroupFilterPositive: Authentication failed for " + user + ",user expected to pass groupfilter");
    }
  }

  @Test
  public void testUserAndGroupFilterNegative() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "uid=%s,ou=Groups,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userFilter", USER1.getUID() + "," + USER2.getUID());
    ldapProperties.put("hive.server2.authentication.ldap.groupFilter", "group1");
    initLdapAtn();

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testUserAndGroupFilterNegative: Authentication succeeded for " + user + ",user expected to fail groupfilter");

      user = USER2.getUID();
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testUserAndGroupFilterNegative: Authentication succeeded for " + user + ",user expected to fail groupfilter");

      user = USER3.getUID();
      ldapProvider.Authenticate(user, USER3.getPassword());
      Assert.fail("testUserAndGroupFilterNegative: Authentication succeeded for " + user + ",user expected to fail groupfilter");
    } catch (AuthenticationException e) {
      assertTrue("testUserAndGroupFilterNegative: Authentication failed for " + user + " as expected", true);
    }
  }

  @Test
  public void testCustomQueryPositive() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "cn=%s,ou=People,dc=example,dc=com:uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "cn=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.customLDAPQuery", "(&(objectClass=person)(|(uid="
                       + USER1.getUID() + ")(uid=" + USER4.getUID() + ")))");
    initLdapAtn();

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testCustomQueryPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER1.getUID();
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testCustomQueryPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER4.getDN();
      ldapProvider.Authenticate(user, USER4.getPassword());
      assertTrue("testCustomQueryPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testCustomQueryPositive: Authentication failed for " + user + ",user expected to pass custom LDAP Query");
    }
  }

  @Test
  public void testCustomQueryNegative() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "ou=People,dc=example,dc=com");
    // ldap query will only return user1
    ldapProperties.put("hive.server2.authentication.ldap.customLDAPQuery", "(&(objectClass=person)(uid="
                       + USER1.getUID() + "))");
    initLdapAtn();

    user = USER2.getDN();
    try {
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testCustomQueryNegative: Authentication succeeded for " + user + ",user expected to fail custom LDAP Query");
    } catch (AuthenticationException e) {
      assertTrue("testCustomQueryNegative: Authentication failed for " + user + " as expected", true);
    }

    try {
      user = USER2.getUID();
      ldapProvider.Authenticate(user, USER2.getPassword());
      Assert.fail("testCustomQueryNegative: Authentication succeeded for " + user + ",user expected to fail custom LDAP Query");
    } catch (AuthenticationException e) {
      assertTrue("testCustomQueryNegative: Authentication failed for " + user + " as expected", true);
    }
  }

  /**
   Test to test the LDAP Atn to use a custom LDAP query that returns
   a) A set of group DNs
   b) A combination of group(s) DN and user DN
   LDAP atn is expected to extract the members of the group using the attribute value for
   "hive.server2.authentication.ldap.groupMembershipKey"
   */
  @Test
  public void testCustomQueryWithGroupsPositive() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "cn=%s,ou=People,dc=example,dc=com:uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.customLDAPQuery",
                         "(&(objectClass=groupOfNames)(|(cn=group1)(cn=group2)))");
    initLdapAtn();

    user = USER1.getDN();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testCustomQueryWithGroupsPositive: Authentication succeeded for " + user + " as expected", true);

       user = USER2.getUID();
       ldapProvider.Authenticate(user, USER2.getPassword());
       assertTrue("testCustomQueryWithGroupsPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testCustomQueryWithGroupsPositive: Authentication failed for " + user + ",user expected to pass custom LDAP Query");
    }

    /* the following test uses a query that returns a group and a user entry.
       the ldap atn should use the groupMembershipKey to identify the users for the returned group
       and the authentication should succeed for the users of that group as well as the lone user4 in this case
    */
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "cn=%s,ou=People,dc=example,dc=com:uid=%s,ou=People,dc=example,dc=com");
    // following query should return group1 and user2
    ldapProperties.put("hive.server2.authentication.ldap.customLDAPQuery",
                         "(|(&(objectClass=groupOfNames)(cn=group1))(&(objectClass=person)(sn=user4)))");
    initLdapAtn();

    user = USER1.getUID();
    try {
      ldapProvider.Authenticate(user, USER1.getPassword());
      assertTrue("testCustomQueryWithGroupsPositive: Authentication succeeded for " + user + " as expected", true);

       user = USER4.getUID();
       ldapProvider.Authenticate(user, USER4.getPassword());
       assertTrue("testCustomQueryWithGroupsPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testCustomQueryWithGroupsPositive: Authentication failed for " + user + ",user expected to pass custom LDAP Query");
    }

    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "cn=%s,ou=People,dc=example,dc=com:uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupMembershipKey", "uniqueMember");
    ldapProperties.put("hive.server2.authentication.ldap.customLDAPQuery",
                         "(&(objectClass=groupOfUniqueNames)(cn=group4))");
    initLdapAtn();

    user = USER4.getDN();
    try {
      ldapProvider.Authenticate(user, USER4.getPassword());
      assertTrue("testCustomQueryWithGroupsPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER4.getUID();
      ldapProvider.Authenticate(user, USER4.getPassword());
      assertTrue("testCustomQueryWithGroupsPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testCustomQueryWithGroupsPositive: Authentication failed for " + user + ",user expected to pass custom LDAP Query");
    }
  }

  @Test
  public void testCustomQueryWithGroupsNegative() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.baseDN", "dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "cn=%s,ou=People,dc=example,dc=com:uid=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.customLDAPQuery",
                         "(&(objectClass=groupOfNames)(|(cn=group1)(cn=group2)))");
    initLdapAtn();

    user = USER3.getDN();
    try {
      ldapProvider.Authenticate(user, USER3.getPassword());
      Assert.fail("testCustomQueryNegative: Authentication succeeded for " + user + ",user expected to fail custom LDAP Query");
    } catch (AuthenticationException e) {
      assertTrue("testCustomQueryNegative: Authentication failed for " + user + " as expected", true);
    }

    try {
      user = USER3.getUID();
      ldapProvider.Authenticate(user, USER3.getPassword());
      Assert.fail("testCustomQueryNegative: Authentication succeeded for " + user + ",user expected to fail custom LDAP Query");
    } catch (AuthenticationException e) {
      assertTrue("testCustomQueryNegative: Authentication failed for " + user + " as expected", true);
    }
  }

  @Test
  public void testGroupFilterPositiveWithCustomGUID() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "cn=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "cn=%s,ou=Groups,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.guidKey", "cn");
    ldapProperties.put("hive.server2.authentication.ldap.groupFilter", "group3");
    initLdapAtn();

    user = USER3.getDN();
    try {
      ldapProvider.Authenticate(user, USER3.getPassword());
      assertTrue("testGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER3.getUID();
      ldapProvider.Authenticate(user, USER3.getPassword());
      assertTrue("testGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testGroupFilterPositive: Authentication failed for " + user + ",user expected to pass groupfilter");
    }
  }

  @Test
  public void testGroupFilterPositiveWithCustomAttributes() throws Exception {
    String user;
    ldapProperties.put("hive.server2.authentication.ldap.userDNPattern", "cn=%s,ou=People,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupDNPattern", "cn=%s,ou=Groups,dc=example,dc=com");
    ldapProperties.put("hive.server2.authentication.ldap.groupFilter", "group4");
    ldapProperties.put("hive.server2.authentication.ldap.guidKey", "cn");
    ldapProperties.put("hive.server2.authentication.ldap.groupMembershipKey", "uniqueMember");
    ldapProperties.put("hive.server2.authentication.ldap.groupClassKey", "groupOfUniqueNames");
    initLdapAtn();

    user = USER4.getDN();
    try {
      ldapProvider.Authenticate(user, USER4.getPassword());
      assertTrue("testGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);

      user = USER4.getUID();
      ldapProvider.Authenticate(user, USER4.getPassword());
      assertTrue("testGroupFilterPositive: Authentication succeeded for " + user + " as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testGroupFilterPositive: Authentication failed for " + user + ",user expected to pass groupfilter");
    }

  }

  private static class User {
    String uid;
    String pwd;
    String ldapDN;

    User(String uid, String password, String ldapDN) {
      this.uid    = uid;
      this.pwd    = password;
      this.ldapDN = ldapDN;
    }

    public String getUID() {
      return uid;
    }

    public String getPassword() {
      return pwd;
    }

    public String getDN() {
      return ldapDN;
    }
  }
}

