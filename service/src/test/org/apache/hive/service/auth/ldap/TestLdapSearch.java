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
package org.apache.hive.service.auth.ldap;

import com.google.common.base.Joiner;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.apache.hive.service.auth.ldap.LdapTestUtils.*;

@RunWith(MockitoJUnitRunner.class)
public class TestLdapSearch {

  @Mock
  private DirContext ctx;

  private HiveConf conf;
  private LdapSearch search;

  @Before
  public void setup() {
    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERMEMBERSHIP_KEY, "memberOf");
  }

  @Test
  public void testClose() throws NamingException {
    search = new LdapSearch(conf, ctx);
    search.close();
    verify(ctx, atLeastOnce()).close();
  }

  @Test
  public void testFindUserDnWhenUserDnPositive() throws NamingException {
    NamingEnumeration<SearchResult> searchResult = mockNamingEnumeration("CN=User1,OU=org1,DC=foo,DC=bar");
    when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(searchResult)
        .thenThrow(NamingException.class);
    search = new LdapSearch(conf, ctx);
    String expected = "CN=User1,OU=org1,DC=foo,DC=bar";
    String actual = search.findUserDn("CN=User1,OU=org1");
    assertEquals(expected, actual);
  }

  @Test
  public void testFindUserDnWhenUserDnNegativeDuplicates() throws NamingException {
    NamingEnumeration<SearchResult> searchResult = mockNamingEnumeration(
            "CN=User1,OU=org1,DC=foo,DC=bar",
            "CN=User1,OU=org2,DC=foo,DC=bar");
    when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(searchResult);
    search = new LdapSearch(conf, ctx);
    assertNull(search.findUserDn("CN=User1,DC=foo,DC=bar"));
  }

  @Test
  public void testFindUserDnWhenUserDnNegativeNone() throws NamingException {
    NamingEnumeration<SearchResult> searchResult = mockEmptyNamingEnumeration();
    when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(searchResult);
    search = new LdapSearch(conf, ctx);
    assertNull(search.findUserDn("CN=User1,DC=foo,DC=bar"));
  }

  @Test
  public void testFindUserDnWhenUserPatternFoundBySecondPattern() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar:CN=%s,OU=org2,DC=foo,DC=bar");
    NamingEnumeration<SearchResult> emptyResult = mockEmptyNamingEnumeration();
    NamingEnumeration<SearchResult> validResult = mockNamingEnumeration("CN=User1,OU=org2,DC=foo,DC=bar");
    when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(emptyResult)
        .thenReturn(validResult);
    search = new LdapSearch(conf, ctx);
    String expected = "CN=User1,OU=org2,DC=foo,DC=bar";
    String actual = search.findUserDn("User1");
    assertEquals(expected, actual);
    verify(ctx).search(eq("OU=org1,DC=foo,DC=bar"), contains("CN=User1"), any(SearchControls.class));
    verify(ctx).search(eq("OU=org2,DC=foo,DC=bar"), contains("CN=User1"), any(SearchControls.class));
  }

  @Test
  public void testFindUserDnWhenUserPatternFoundByFirstPattern() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar:CN=%s,OU=org2,DC=foo,DC=bar");
    NamingEnumeration<SearchResult> emptyResult = mockEmptyNamingEnumeration();
    NamingEnumeration<SearchResult> validResult = mockNamingEnumeration("CN=User1,OU=org2,DC=foo,DC=bar");
    when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(validResult)
        .thenReturn(emptyResult);
    search = new LdapSearch(conf, ctx);
    String expected = "CN=User1,OU=org2,DC=foo,DC=bar";
    String actual = search.findUserDn("User1");
    assertEquals(expected, actual);
    verify(ctx).search(eq("OU=org1,DC=foo,DC=bar"), contains("CN=User1"), any(SearchControls.class));
  }

  @Test
  public void testFindUserDnWhenUserPatternFoundByUniqueIdentifier() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar");
    NamingEnumeration<SearchResult> validResult = mockNamingEnumeration("CN=User1,OU=org1,DC=foo,DC=bar");
    when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(null)
        .thenReturn(validResult);
    search = new LdapSearch(conf, ctx);
    String expected = "CN=User1,OU=org1,DC=foo,DC=bar";
    String actual = search.findUserDn("User1");
    assertEquals(expected, actual);
    verify(ctx).search(eq("OU=org1,DC=foo,DC=bar"), contains("CN=User1"), any(SearchControls.class));
    verify(ctx).search(eq("OU=org1,DC=foo,DC=bar"), contains("uid=User1"), any(SearchControls.class));
  }

  @Test
  public void testFindUserDnWhenUserPatternFoundByUniqueIdentifierNegativeNone() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar");
    when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(null)
        .thenReturn(null);
    search = new LdapSearch(conf, ctx);
    assertNull(search.findUserDn("User1"));
  }

  @Test
  public void testFindUserDnWhenUserPatternFoundByUniqueIdentifierNegativeMany() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar");
    NamingEnumeration<SearchResult> manyResult = mockNamingEnumeration(
        "CN=User1,OU=org1,DC=foo,DC=bar",
        "CN=User12,OU=org1,DC=foo,DC=bar");
    when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(null)
        .thenReturn(manyResult);
    search = new LdapSearch(conf, ctx);
    assertNull(search.findUserDn("User1"));
  }

  @Test
  public void testFindGroupsForUser() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar");

    NamingEnumeration<SearchResult> groupsResult = mockNamingEnumeration("CN=Group1,OU=org1,DC=foo,DC=bar");
    when(ctx.search(eq("OU=org1,DC=foo,DC=bar"), contains("User1"), any(SearchControls.class)))
        .thenReturn(groupsResult);

    search = new LdapSearch(conf, ctx);

    List<String> expected = Arrays.asList("CN=Group1,OU=org1,DC=foo,DC=bar");
    List<String> actual = search.findGroupsForUser("CN=User1,OU=org1,DC=foo,DC=bar");
    assertEquals(expected, actual);
  }

  @Test
  public void testExecuteCustomQuery() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_BASEDN, "dc=example,dc=com");

    NamingEnumeration<SearchResult> customQueryResult = mockNamingEnumeration(
        mockSearchResult(
            "uid=group1,ou=Groups,dc=example,dc=com",
            mockAttributes("member", "uid=user1,ou=People,dc=example,dc=com")),
        mockSearchResult(
            "uid=group2,ou=Groups,dc=example,dc=com",
            mockAttributes("member", "uid=user2,ou=People,dc=example,dc=com"))
        );

    when(ctx.search(eq("dc=example,dc=com"), anyString(), any(SearchControls.class)))
        .thenReturn(customQueryResult);

    search = new LdapSearch(conf, ctx);

    List<String> expected = Arrays.asList(
        "uid=group1,ou=Groups,dc=example,dc=com",
        "uid=user1,ou=People,dc=example,dc=com",
        "uid=group2,ou=Groups,dc=example,dc=com",
        "uid=user2,ou=People,dc=example,dc=com");
    List<String> actual = search.executeCustomQuery("(&(objectClass=groupOfNames)(|(cn=group1)(cn=group2)))");
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testFindGroupDnPositive() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar");
    String groupDn = "CN=Group1";
    NamingEnumeration<SearchResult> result = mockNamingEnumeration(groupDn);
    when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(result);
    search = new LdapSearch(conf, ctx);
    String expected = groupDn;
    String actual = search.findGroupDn("grp1");
    assertEquals(expected, actual);
  }

  @Test(expected = NamingException.class)
  public void testFindGroupDNNoResults() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar");
    NamingEnumeration<SearchResult> result = mockEmptyNamingEnumeration();
    when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(result);
    search = new LdapSearch(conf, ctx);
    search.findGroupDn("anyGroup");
  }

  @Test(expected = NamingException.class)
  public void testFindGroupDNTooManyResults() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar");
    NamingEnumeration<SearchResult> result =
        LdapTestUtils.mockNamingEnumeration("Result1", "Result2", "Result3");
    when(ctx.search(anyString(), anyString(), any(SearchControls.class))).thenReturn(result);
    search = new LdapSearch(conf, ctx);
    search.findGroupDn("anyGroup");
  }

  @Test
  public void testFindGroupDNWhenExceptionInSearch() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPDNPATTERN,
        Joiner.on(":").join(
            "CN=%s,OU=org1,DC=foo,DC=bar",
            "CN=%s,OU=org2,DC=foo,DC=bar"
        )
    );
    NamingEnumeration<SearchResult> result = LdapTestUtils.mockNamingEnumeration("CN=Group1");
    when(ctx.search(anyString(), anyString(), any(SearchControls.class)))
        .thenReturn(result)
        .thenThrow(NamingException.class);
    search = new LdapSearch(conf, ctx);
    String expected = "CN=Group1";
    String actual = search.findGroupDn("grp1");
    assertEquals(expected, actual);
  }

  @Test
  public void testIsUserMemberOfGroupWhenUserId() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar");
    NamingEnumeration<SearchResult> validResult = LdapTestUtils.mockNamingEnumeration("CN=User1");
    NamingEnumeration<SearchResult> emptyResult = LdapTestUtils.mockEmptyNamingEnumeration();
    when(ctx.search(anyString(), contains("(uid=usr1)"), any(SearchControls.class)))
        .thenReturn(validResult);
    when(ctx.search(anyString(), contains("(uid=usr2)"), any(SearchControls.class)))
        .thenReturn(emptyResult);
    search = new LdapSearch(conf, ctx);
    assertTrue(search.isUserMemberOfGroup("usr1", "grp1"));
    assertFalse(search.isUserMemberOfGroup("usr2", "grp2"));
  }

  @Test
  public void testIsUserMemberOfGroupWhenUserDn() throws NamingException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_USERDNPATTERN,
        "CN=%s,OU=org1,DC=foo,DC=bar");
    NamingEnumeration<SearchResult> validResult = LdapTestUtils.mockNamingEnumeration("CN=User1");
    NamingEnumeration<SearchResult> emptyResult = LdapTestUtils.mockEmptyNamingEnumeration();
    when(ctx.search(anyString(), contains("(uid=User1)"), any(SearchControls.class)))
        .thenReturn(validResult);
    when(ctx.search(anyString(), contains("(uid=User2)"), any(SearchControls.class)))
        .thenReturn(emptyResult);
    search = new LdapSearch(conf, ctx);
    assertTrue(search.isUserMemberOfGroup("CN=User1,OU=org1,DC=foo,DC=bar", "grp1"));
    assertFalse(search.isUserMemberOfGroup("CN=User2,OU=org1,DC=foo,DC=bar", "grp2"));
  }
}
