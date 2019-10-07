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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestQueryFactory {

  private QueryFactory queries;
  private Configuration conf;

  @Before
  public void setup() {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GUIDKEY, "guid");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPCLASS_KEY, "superGroups");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_GROUPMEMBERSHIP_KEY, "member");
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_PLAIN_LDAP_USERMEMBERSHIP_KEY, "partOf");
    queries = new QueryFactory(conf);
  }

  @Test
  public void testFindGroupDnById() {
    Query q = queries.findGroupDnById("unique_group_id");
    String expected = "(&(objectClass=superGroups)(guid=unique_group_id))";
    String actual = q.getFilter();
    assertEquals(expected, actual);
  }

  @Test
  public void testFindUserDnByRdn() {
    Query q = queries.findUserDnByRdn("cn=User1");
    String expected = "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))(cn=User1))";
    String actual = q.getFilter();
    assertEquals(expected, actual);
  }

  @Test
  public void testFindDnByPattern() {
    Query q = queries.findDnByPattern("cn=User1");
    String expected = "(cn=User1)";
    String actual = q.getFilter();
    assertEquals(expected, actual);
  }

  @Test
  public void testFindUserDnByName() {
    Query q = queries.findUserDnByName("unique_user_id");
    String expected = "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))(|(uid=unique_user_id)(sAMAccountName=unique_user_id)))";
    String actual = q.getFilter();
    assertEquals(expected, actual);
  }

  @Test
  public void testFindGroupsForUser() {
    Query q = queries.findGroupsForUser("user_name", "user_Dn");
    String expected = "(&(objectClass=superGroups)(|(member=user_Dn)(member=user_name)))";
    String actual = q.getFilter();
    assertEquals(expected, actual);
  }

  @Test
  public void testIsUserMemberOfGroup() {
    Query q = queries.isUserMemberOfGroup("unique_user", "cn=MyGroup,ou=Groups,dc=mycompany,dc=com");
    String expected = "(&(|(objectClass=person)(objectClass=user)(objectClass=inetOrgPerson))" +
         "(partOf=cn=MyGroup,ou=Groups,dc=mycompany,dc=com)(guid=unique_user))";
    String actual = q.getFilter();
    assertEquals(expected, actual);
  }

  @Test(expected = IllegalStateException.class)
  public void testIsUserMemberOfGroupWhenMisconfigured() {
    QueryFactory misconfiguredQueryFactory = new QueryFactory(MetastoreConf.newMetastoreConf());
    misconfiguredQueryFactory.isUserMemberOfGroup("user", "cn=MyGroup");
  }

  @Test
  public void testFindGroupDNByID() {
    Query q = queries.findGroupDnById("unique_group_id");
    String expected = "(&(objectClass=superGroups)(guid=unique_group_id))";
    String actual = q.getFilter();
    assertEquals(expected, actual);
  }
}
