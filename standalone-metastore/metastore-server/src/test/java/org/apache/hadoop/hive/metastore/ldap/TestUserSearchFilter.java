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

import java.io.IOException;
import javax.naming.NamingException;
import javax.security.sasl.AuthenticationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestUserSearchFilter {

  private FilterFactory factory;
  private Configuration conf;

  @Mock
  private DirSearch search;

  @Before
  public void setup() {
    conf = MetastoreConf.newMetastoreConf();
    factory = new UserSearchFilterFactory();
  }

  @Test
  public void testFactoryWhenNoGroupOrUserFilters() {
    assertNull(factory.getInstance(conf));
  }

  @Test
  public void testFactoryWhenGroupFilter() {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPFILTER, "Grp1,Grp2");
    assertNotNull(factory.getInstance(conf));
  }

  @Test
  public void testFactoryWhenUserFilter() {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "User1,User2");
    assertNotNull(factory.getInstance(conf));
  }

  @Test
  public void testApplyPositive() throws AuthenticationException, NamingException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "User1");
    Filter filter = factory.getInstance(conf);

    when(search.findUserDn(anyString())).thenReturn("cn=User1,ou=People,dc=example,dc=com");

    filter.apply(search, "User1");
  }

  @Test(expected = AuthenticationException.class)
  public void testApplyWhenNamingException() throws AuthenticationException, NamingException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "User1");
    Filter filter = factory.getInstance(conf);

    when(search.findUserDn(anyString())).thenThrow(NamingException.class);

    filter.apply(search, "User3");
  }

  @Test(expected = AuthenticationException.class)
  public void testApplyWhenNotFound() throws AuthenticationException, NamingException, IOException {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERFILTER, "User1");
    Filter filter = factory.getInstance(conf);

    when(search.findUserDn(anyString())).thenReturn(null);

    filter.apply(search, "User3");
  }
}
