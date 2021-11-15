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

import java.io.IOException;
import java.util.Arrays;
import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import org.junit.Before;
import org.mockito.Mock;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestCustomQueryFilter {

  private static final String USER2_DN = "uid=user2,ou=People,dc=example,dc=com";
  private static final String USER1_DN = "uid=user1,ou=People,dc=example,dc=com";
  private static final String CUSTOM_QUERY = "(&(objectClass=person)(|(uid=user1)(uid=user2)))";

  private FilterFactory factory;
  private HiveConf conf;

  @Mock
  private DirSearch search;

  @Before
  public void setup() {
    conf = new HiveConf();
    conf.set("hive.root.logger", "DEBUG,console");
    factory = new CustomQueryFilterFactory();
  }

  @Test
  public void testFactory() {
    conf.unset(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_CUSTOMLDAPQUERY.varname);
    assertNull(factory.getInstance(conf));

    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_CUSTOMLDAPQUERY, CUSTOM_QUERY);
    assertNotNull(factory.getInstance(conf));
  }

  @Test
  public void testApplyPositive() throws AuthenticationException, NamingException, IOException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_CUSTOMLDAPQUERY, CUSTOM_QUERY);

    when(search.executeCustomQuery(eq(CUSTOM_QUERY))).thenReturn(Arrays.asList(USER1_DN, USER2_DN));

    Filter filter = factory.getInstance(conf);
    filter.apply(search, "user1");
    filter.apply(search, "user2");
  }


  @Test(expected = AuthenticationException.class)
  public void testApplyNegative() throws AuthenticationException, NamingException, IOException {
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_PLAIN_LDAP_CUSTOMLDAPQUERY, CUSTOM_QUERY);

    when(search.executeCustomQuery(eq(CUSTOM_QUERY))).thenReturn(Arrays.asList(USER1_DN, USER2_DN));

    Filter filter = factory.getInstance(conf);
    filter.apply(search, "user3");
  }
}
