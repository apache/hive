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
package org.apache.hive.service.auth.ldap;

import java.io.IOException;
import java.util.Arrays;
import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import org.junit.Before;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestGroupFilter {

  private FilterFactory factory;
  private HiveConf conf;

  @Mock
  private DirSearch search;

  @Before
  public void setup() {
    conf = new HiveConf();
    conf.set("hive.root.logger", "DEBUG,console");
    factory = new GroupFilterFactory();
  }

  @Test
  public void testFactory() {
    conf.unset(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER.varname);
    assertNull(factory.getInstance(conf));

    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "G1");
    assertNotNull(factory.getInstance(conf));
  }

  @Test
  public void testApplyPositive() throws AuthenticationException, NamingException, IOException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "HiveUsers");

    when(search.findUserDn(eq("user1")))
        .thenReturn("cn=user1,ou=People,dc=example,dc=com");
    when(search.findUserDn(eq("cn=user2,dc=example,dc=com")))
        .thenReturn("cn=user2,ou=People,dc=example,dc=com");
    when(search.findUserDn(eq("user3@mydomain.com")))
        .thenReturn("cn=user3,ou=People,dc=example,dc=com");

    when(search.findGroupsForUser(eq("cn=user1,ou=People,dc=example,dc=com")))
        .thenReturn(Arrays.asList(
            "cn=SuperUsers,ou=Groups,dc=example,dc=com",
            "cn=Office1,ou=Groups,dc=example,dc=com",
            "cn=HiveUsers,ou=Groups,dc=example,dc=com",
            "cn=G1,ou=Groups,dc=example,dc=com"));
    when(search.findGroupsForUser(eq("cn=user2,ou=People,dc=example,dc=com")))
        .thenReturn(Arrays.asList(
            "cn=HiveUsers,ou=Groups,dc=example,dc=com"));
    when(search.findGroupsForUser(eq("cn=user3,ou=People,dc=example,dc=com")))
        .thenReturn(Arrays.asList(
            "cn=HiveUsers,ou=Groups,dc=example,dc=com",
            "cn=G1,ou=Groups,dc=example,dc=com",
            "cn=G2,ou=Groups,dc=example,dc=com"));

    Filter filter = factory.getInstance(conf);
    filter.apply(search, "user1");
    filter.apply(search, "cn=user2,dc=example,dc=com");
    filter.apply(search, "user3@mydomain.com");
  }

  @Test(expected = AuthenticationException.class)
  public void testApplyNegative() throws AuthenticationException, NamingException, IOException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_GROUPFILTER, "HiveUsers");

    when(search.findGroupsForUser(eq("user1"))).thenReturn(Arrays.asList("SuperUsers", "Office1", "G1", "G2"));

    Filter filter = factory.getInstance(conf);
    filter.apply(search, "user1");
  }
}
