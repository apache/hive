/**
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

package org.apache.hive.service.auth;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig;
import com.unboundid.ldap.listener.InMemoryListenerConfig;
import com.unboundid.ldap.sdk.DN;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldif.LDIFReader;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

import javax.security.sasl.AuthenticationException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests that use an in-memory LDAP Server (unboundID) to test HS2's
 * LDAP Authentication Provider. The ldap server uses a sample ldif
 * file to load ldap data into its directory.
 * Any of Hive's LDAP Configuration properties are set on the HiveConf
 * prior to the initialization of LdapAuthenticationProviderImpl.
 * Each test uses a different set of properties to alter the Atn
 * provider behavior.
 */
public class TestLdapAtnProviderWithLdapServer {
  private static String ldapUrl;
  private static InMemoryDirectoryServer server;
  private static InMemoryDirectoryServerConfig config;
  private static HiveConf hiveConf;
  private static byte[] hiveConfBackup;
  private static LdapAuthenticationProviderImpl ldapProvider;
  private static final int serverPort = 33300;

  @BeforeClass
  public static void init() throws Exception {
    DN dn = new DN("dc=example, dc=com");
    config = new InMemoryDirectoryServerConfig(dn);
    config.setSchema(null);
    config.addAdditionalBindCredentials("cn=user1,ou=People,dc=example,dc=com","user1");
    config.addAdditionalBindCredentials("cn=user2,ou=People,dc=example,dc=com","user2");

    // listener config only necessary if you want to make sure that the
    // server listens on port 33300, otherwise a free random port will
    // be picked at runtime - which might be even better for tests btw.
    config.setListenerConfigs(
            new InMemoryListenerConfig("myListener", null, serverPort, null, null, null));

    server = new InMemoryDirectoryServer(config);

    server.startListening();

    File ldifFile = new File(Thread.currentThread().getContextClassLoader()
                       .getResource("org/apache/hive/service/auth/ldapdata.ldif").getFile());
    LDIFReader ldifReader = new LDIFReader(ldifFile);
    // import your test data from ldif files
    server.importFromLDIF(true, ldifReader);

    LDAPConnection conn = server.getConnection();
    int port = server.getListenPort();
    ldapUrl = new String("ldap://localhost:" + port);

    hiveConf = new HiveConf();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    hiveConf.writeXml(baos);
    baos.close();
    hiveConfBackup = baos.toByteArray();
    hiveConf.set("hive.root.logger", "TRACE,console");
    hiveConf.set("hive.server2.authentication.ldap.url", ldapUrl);
    hiveConf.set("hive.server2.authentication.ldap.baseDN", "dc=example,dc=com");
    hiveConf.set("hive.server2.authentication.ldap.userDNPattern", "cn=%s,ou=People,dc=example,dc=com");
    FileOutputStream fos = new FileOutputStream(new File(hiveConf.getHiveSiteLocation().toURI()));
    hiveConf.writeXml(fos);
    fos.close();

    ldapProvider = new LdapAuthenticationProviderImpl();
  }

  private static void initLdapAtn(Hashtable<String, String> hiveProperties)
        throws Exception {
    Set<String> keys = hiveProperties.keySet();
    Iterator<String> iter = keys.iterator();
    hiveConf = new HiveConf();

    try {
      boolean deleted = new File(hiveConf.getHiveSiteLocation().toURI()).delete();
    } catch (Exception e) {}

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    hiveConf.writeXml(baos);
    baos.close();

    hiveConf.set("hive.root.logger", "TRACE,console");
    hiveConf.set("hive.server2.authentication.ldap.url", ldapUrl);
    hiveConf.set("hive.server2.authentication.ldap.userDNPattern", "cn=%s,ou=People,dc=example,dc=com");
    hiveConf.set("hive.server2.authentication.ldap.groupDNPattern", "cn=%s,ou=Groups,dc=example,dc=com");

    String key;
    String value;
    while (iter.hasNext()) {
      key = iter.next();
      value = hiveProperties.get(key);
      hiveConf.set(key, value);
    }

    FileOutputStream fos = new FileOutputStream(new File(hiveConf.getHiveSiteLocation().toURI()));
    hiveConf.writeXml(fos);
    fos.close();

    ldapProvider = new LdapAuthenticationProviderImpl();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    server.shutDown(true);
  }

  @Test
  public void testRoot() throws Exception {
    Hashtable<String, String> ldapProperties = new Hashtable<String, String>();
    initLdapAtn(ldapProperties);
    String user;

    user = "cn=user1,ou=People,dc=example,dc=com";
    try {
      ldapProvider.Authenticate(user, "user1");
      assertTrue(true);

      user = "cn=user2,ou=People,dc=example,dc=com";
      ldapProvider.Authenticate(user, "user2");
      assertTrue(true);
    } catch (AuthenticationException e) {
      e.printStackTrace();
      Assert.fail("Authentication failed for user:" + user);
    }
  }

  @Test
  public void testUserBindPositive() throws Exception {
    Hashtable<String, String> ldapProperties = new Hashtable<String, String>();
    ldapProperties.put("hive.server2.authentication.ldap.userFilter", "user1,user2");
    initLdapAtn(ldapProperties);
    String user;

    user = "cn=user1,ou=People,dc=example,dc=com";
    try {
      ldapProvider.Authenticate(user, "user1");
      assertTrue("testUserBindPositive: Authentication succeeded for user1 as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password user1, expected to succeed");
    }

    user = "cn=user2,ou=People,dc=example,dc=com";
    try {
      ldapProvider.Authenticate(user, "user2");
      assertTrue("testUserBindPositive: Authentication succeeded for user2 as expected", true);
    } catch (AuthenticationException e) {
      Assert.fail("testUserBindPositive: Authentication failed for user:" + user +
                    " with password user2, expected to succeed");
    }
  }

  @Test
  public void testUserBindNegative() throws Exception {
    Hashtable<String, String> ldapProperties = new Hashtable<String, String>();
    initLdapAtn(ldapProperties);

    try {
      ldapProvider.Authenticate("cn=user1,ou=People,dc=example,dc=com", "user2");
      Assert.fail("testUserBindNegative: Authentication succeeded for user1 with password " +
                   "user2, expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for user1 as expected", true);
    }

    try {
      ldapProvider.Authenticate("cn=user2,ou=People,dc=example,dc=com", "user");
      Assert.fail("testUserBindNegative: Authentication failed for user2 with password user, " +
                    "expected to fail");
    } catch (AuthenticationException e) {
      assertTrue("testUserBindNegative: Authentication failed for user2 as expected", true);
    }
  }
}
