/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestACLConfigurationParser {


  @Test (timeout = 10_000L)
  public void test() {

    ACLConfigurationParser aclConf;
    Configuration conf = new Configuration();
    conf.set("ACL_ALL_ACCESS", "*");
    aclConf = new ACLConfigurationParser(conf, "ACL_ALL_ACCESS");
    assertEquals(1, aclConf.getAllowedUsers().size());
    assertTrue(aclConf.getAllowedUsers().contains("*"));
    assertEquals(0, aclConf.getAllowedGroups().size());
    assertEquals("*", aclConf.toAclString());

    conf.set("ACL_INVALID1", "u1, u2, u3");
    aclConf = new ACLConfigurationParser(conf, "ACL_INVALID1");
    assertEquals(0, aclConf.getAllowedUsers().size());
    assertEquals(0, aclConf.getAllowedGroups().size());
    assertEquals(" ", aclConf.toAclString());

    conf.set("ACL_NONE", " ");
    aclConf = new ACLConfigurationParser(conf, "ACL_NONE");
    assertEquals(0, aclConf.getAllowedUsers().size());
    assertEquals(0, aclConf.getAllowedGroups().size());
    assertEquals(" ", aclConf.toAclString());

    conf.set("ACL_VALID1", "user1,user2");
    aclConf = new ACLConfigurationParser(conf, "ACL_VALID1");
    assertEquals(2, aclConf.getAllowedUsers().size());
    assertTrue(aclConf.getAllowedUsers().contains("user1"));
    assertTrue(aclConf.getAllowedUsers().contains("user2"));
    assertEquals(0, aclConf.getAllowedGroups().size());
    assertEquals("user1,user2", aclConf.toAclString());

    conf.set("ACL_VALID2", "user1,user2 group1,group2");
    aclConf = new ACLConfigurationParser(conf, "ACL_VALID2");
    assertEquals(2, aclConf.getAllowedUsers().size());
    assertTrue(aclConf.getAllowedUsers().contains("user1"));
    assertTrue(aclConf.getAllowedUsers().contains("user2"));
    assertEquals(2, aclConf.getAllowedGroups().size());
    assertTrue(aclConf.getAllowedGroups().contains("group1"));
    assertTrue(aclConf.getAllowedGroups().contains("group2"));
    assertEquals("user1,user2 group1,group2", aclConf.toAclString());


    conf.set("ACL_VALID3", "user1 group1");
    aclConf = new ACLConfigurationParser(conf, "ACL_VALID3");
    assertEquals(1, aclConf.getAllowedUsers().size());
    assertTrue(aclConf.getAllowedUsers().contains("user1"));
    assertEquals(1, aclConf.getAllowedGroups().size());
    assertTrue(aclConf.getAllowedGroups().contains("group1"));
    assertEquals("user1 group1", aclConf.toAclString());

    aclConf.addAllowedUser("user2");
    assertEquals(2, aclConf.getAllowedUsers().size());
    assertTrue(aclConf.getAllowedUsers().contains("user1"));
    assertTrue(aclConf.getAllowedUsers().contains("user2"));
    assertEquals("user1,user2 group1", aclConf.toAclString());

    aclConf.addAllowedGroup("group2");
    assertEquals(2, aclConf.getAllowedGroups().size());
    assertTrue(aclConf.getAllowedGroups().contains("group1"));
    assertTrue(aclConf.getAllowedGroups().contains("group2"));
    assertEquals("user1,user2 group1,group2", aclConf.toAclString());

    aclConf.addAllowedUser("*");
    assertEquals(1, aclConf.getAllowedUsers().size());
    assertTrue(aclConf.getAllowedUsers().contains("*"));
    assertTrue(aclConf.getAllowedGroups().isEmpty());
  }

}
