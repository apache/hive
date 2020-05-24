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
package org.apache.hadoop.hive.metastore.ldap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestLdapUtils {

  @Test
  public void testCreateCandidatePrincipalsForUserDn() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    String userDn = "cn=user1,ou=CORP,dc=mycompany,dc=com";
    List<String> expected = Arrays.asList(userDn);
    List<String> actual = LdapUtils.createCandidatePrincipals(conf, userDn);
    assertEquals(expected, actual);
  }

  @Test
  public void testCreateCandidatePrincipalsForUserWithDomain() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    String userWithDomain = "user1@mycompany.com";
    List<String> expected = Arrays.asList(userWithDomain);
    List<String> actual = LdapUtils.createCandidatePrincipals(conf, userWithDomain);
    assertEquals(expected, actual);
  }

  @Test
  public void testCreateCandidatePrincipalsLdapDomain() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_DOMAIN, "mycompany.com");
    List<String> expected = Arrays.asList("user1@mycompany.com");
    List<String> actual = LdapUtils.createCandidatePrincipals(conf, "user1");
    assertEquals(expected, actual);
  }

  @Test
  public void testCreateCandidatePrincipalsUserPatternsDefaultBaseDn() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GUIDKEY,
            "sAMAccountName");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BASEDN, "dc=mycompany," +
            "dc=com");
    List<String> expected = Arrays.asList("sAMAccountName=user1,dc=mycompany,dc=com");
    List<String> actual = LdapUtils.createCandidatePrincipals(conf, "user1");
    assertEquals(expected, actual);
  }

  @Test
  public void testCreateCandidatePrincipals() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_BASEDN, "dc=mycompany," +
            "dc=com");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERDNPATTERN,
        "cn=%s,ou=CORP1,dc=mycompany,dc=com:cn=%s,ou=CORP2,dc=mycompany,dc=com");
    List<String> expected = Arrays.asList(
        "cn=user1,ou=CORP1,dc=mycompany,dc=com",
        "cn=user1,ou=CORP2,dc=mycompany,dc=com");
    List<String> actual = LdapUtils.createCandidatePrincipals(conf, "user1");
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testExtractFirstRdn() {
    String dn = "cn=user1,ou=CORP1,dc=mycompany,dc=com";
    String expected = "cn=user1";
    String actual = LdapUtils.extractFirstRdn(dn);
    assertEquals(expected, actual);
  }

  @Test
  public void testExtractBaseDn() {
    String dn = "cn=user1,ou=CORP1,dc=mycompany,dc=com";
    String expected = "ou=CORP1,dc=mycompany,dc=com";
    String actual = LdapUtils.extractBaseDn(dn);
    assertEquals(expected, actual);
  }

  @Test
  public void testExtractBaseDnNegative() {
    String dn = "cn=user1";
    assertNull(LdapUtils.extractBaseDn(dn));
  }
}
