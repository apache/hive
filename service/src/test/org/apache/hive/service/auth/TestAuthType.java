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

package org.apache.hive.service.auth;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.EnumUtils;
import org.apache.hadoop.hive.conf.HiveServer2TransportMode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Tests to test if AuthType can parse allowed values and reject disallowed combinations.
 */
public class TestAuthType {
  @Test
  public void testSingleAuth() throws Exception {
    for (HiveAuthConstants.AuthTypes type : HiveAuthConstants.AuthTypes.values()) {
      testSingleAuth(type);
    }
  }

  private void testSingleAuth(HiveAuthConstants.AuthTypes type) throws Exception {
    AuthType authType = new AuthType(type.getAuthName(), HiveServer2TransportMode.http);
    Assert.assertTrue(authType.isEnabled(type));
    if (type == HiveAuthConstants.AuthTypes.NOSASL || type == HiveAuthConstants.AuthTypes.NONE ||
        AuthType.PASSWORD_BASED_TYPES.contains(type)) {
      Assert.assertEquals(type.getAuthName(), authType.getPasswordBasedAuthStr());
    } else {
      Assert.assertEquals("Should return empty string if no password based authentication is set.",
          "", authType.getPasswordBasedAuthStr());
    }
  }

  @Test
  public void testOnePasswordAuthWithSAML() throws Exception {
    testOnePasswordAuthWithSAML(HiveAuthConstants.AuthTypes.LDAP);
    testOnePasswordAuthWithSAML(HiveAuthConstants.AuthTypes.PAM);
    testOnePasswordAuthWithSAML(HiveAuthConstants.AuthTypes.CUSTOM);
  }

  @Test
  public void testOnePasswordAuthWithJWT() throws Exception {
    testOnePasswordAuthWithJWT(HiveAuthConstants.AuthTypes.LDAP);
    testOnePasswordAuthWithJWT(HiveAuthConstants.AuthTypes.PAM);
    testOnePasswordAuthWithJWT(HiveAuthConstants.AuthTypes.CUSTOM);
  }

  private void testOnePasswordAuthWithSAML(HiveAuthConstants.AuthTypes type) throws Exception {
    AuthType authType = new AuthType("SAML," + type.getAuthName(), HiveServer2TransportMode.http);
    Assert.assertTrue(authType.isEnabled(HiveAuthConstants.AuthTypes.SAML));
    Assert.assertTrue(authType.isEnabled(type));

    Set<HiveAuthConstants.AuthTypes> disabledAuthTypes = Arrays.stream(HiveAuthConstants.AuthTypes.values())
        .collect(Collectors.toSet());
    disabledAuthTypes.remove(HiveAuthConstants.AuthTypes.SAML);
    disabledAuthTypes.remove(type);
    for (HiveAuthConstants.AuthTypes disabledType : disabledAuthTypes) {
      Assert.assertFalse(authType.isEnabled(disabledType));
    }
    Assert.assertEquals(type.getAuthName(), authType.getPasswordBasedAuthStr());

    verify("SAML," + type.getAuthName(), HiveServer2TransportMode.binary, true);
    verify("SAML," + type.getAuthName(), HiveServer2TransportMode.all, true);
  }

  private void verify(String authTypes, HiveServer2TransportMode mode, boolean shouldThrowException) {
    try {
      AuthType authType = new AuthType(authTypes, mode);
      if (shouldThrowException) {
        Assert.fail("HiveServer2 " + mode.name() + " mode cann't support " + authTypes + " by design");
      } else {
        String[] authMethods = authTypes.split(",");
        for (int i = 0; i < authMethods.length; i++) {
          HiveAuthConstants.AuthTypes authMech = EnumUtils.getEnumIgnoreCase(HiveAuthConstants.AuthTypes.class,
              authMethods[i]);
          Assert.assertTrue(authType.isEnabled(authMech));
        }
      }
    } catch (Exception e) {
      if (!shouldThrowException) {
        Assert.fail("HiveServer2 " + mode.name() + " mode should be able to support " + authTypes);
      } else {
        Assert.assertTrue(e instanceof RuntimeException);
      }
    }
  }

  private void testOnePasswordAuthWithJWT(HiveAuthConstants.AuthTypes type) throws Exception {
    AuthType authType = new AuthType("JWT," + type.getAuthName(), HiveServer2TransportMode.http);
    Assert.assertTrue(authType.isEnabled(HiveAuthConstants.AuthTypes.JWT));
    Assert.assertTrue(authType.isEnabled(type));

    Set<HiveAuthConstants.AuthTypes> disabledAuthTypes = Arrays.stream(HiveAuthConstants.AuthTypes.values())
        .collect(Collectors.toSet());
    disabledAuthTypes.remove(HiveAuthConstants.AuthTypes.JWT);
    disabledAuthTypes.remove(type);
    for (HiveAuthConstants.AuthTypes disabledType : disabledAuthTypes) {
      Assert.assertFalse(authType.isEnabled(disabledType));
    }
    Assert.assertEquals(type.getAuthName(), authType.getPasswordBasedAuthStr());
    verify("JWT," + type.getAuthName(), HiveServer2TransportMode.binary, true);
    verify("JWT," + type.getAuthName(), HiveServer2TransportMode.all, true);
  }

  @Test
  public void testMultipleAuthMethods() {
    Set<EntryForTest> entries = ImmutableSet.of(
        new EntryForTest("KERBEROS,SAML", HiveServer2TransportMode.binary, true),
        new EntryForTest("KERBEROS,SAML", HiveServer2TransportMode.http, false),
        new EntryForTest("KERBEROS,SAML,LDAP", HiveServer2TransportMode.all, true),
        new EntryForTest("KERBEROS,SAML,LDAP", HiveServer2TransportMode.http, false),
        new EntryForTest("KERBEROS,LDAP", HiveServer2TransportMode.all, false),
        new EntryForTest("NONE,SAML", HiveServer2TransportMode.all, true),
        new EntryForTest("NONE,SAML", HiveServer2TransportMode.http, true),
        new EntryForTest("NOSASL,SAML", HiveServer2TransportMode.all, true),
        new EntryForTest("SAML,LDAP,PAM,CUSTOM", HiveServer2TransportMode.http, true),
        new EntryForTest("SAML,OTHER", HiveServer2TransportMode.all, true),
        new EntryForTest("LDAP,PAM,CUSTOM", HiveServer2TransportMode.binary, true),
        new EntryForTest("KERBEROS,JWT", HiveServer2TransportMode.binary, true),
        new EntryForTest("KERBEROS,JWT", HiveServer2TransportMode.http, false),
        new EntryForTest("KERBEROS,JWT,LDAP", HiveServer2TransportMode.http, false),
        new EntryForTest("KERBEROS,JWT,LDAP", HiveServer2TransportMode.all, true),
        new EntryForTest("NONE,JWT", HiveServer2TransportMode.all, true),
        new EntryForTest("NOSASL,JWT", HiveServer2TransportMode.http, true),
        new EntryForTest("JWT,LDAP,PAM,CUSTOM", HiveServer2TransportMode.http, true),
        new EntryForTest("JWT,SAML,LDAP", HiveServer2TransportMode.http, false),
        new EntryForTest("JWT,SAML,LDAP", HiveServer2TransportMode.all, true),
        new EntryForTest("JWT,SAML", HiveServer2TransportMode.http, false),
        new EntryForTest("JWT,SAML", HiveServer2TransportMode.binary, true)
    );

    for (EntryForTest entry : entries) {
      verify(entry.authTypes, entry.mode, entry.shouldThrowException);
    }
  }

  private class EntryForTest {
    String authTypes;
    HiveServer2TransportMode mode;
    boolean shouldThrowException;
    EntryForTest(String authTypes, HiveServer2TransportMode mode, boolean shouldThrowException) {
      this.authTypes = authTypes;
      this.mode = mode;
      this.shouldThrowException = shouldThrowException;
    }
  }

}
