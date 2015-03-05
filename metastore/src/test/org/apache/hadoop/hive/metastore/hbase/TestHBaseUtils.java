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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class TestHBaseUtils {

  @Test
  public void privilegeSerialization() throws Exception {
    PrincipalPrivilegeSet pps = new PrincipalPrivilegeSet();
    pps.setUserPrivileges(new HashMap<String, List<PrivilegeGrantInfo>>());
    pps.setRolePrivileges(new HashMap<String, List<PrivilegeGrantInfo>>());

    pps.getUserPrivileges().put("fred", Arrays.asList(new PrivilegeGrantInfo("read", 1, "daphne",
        PrincipalType.USER, true)));
    pps.getUserPrivileges().put("wilma", Arrays.asList(new PrivilegeGrantInfo("write", 1,
        "scooby", PrincipalType.USER, false)));
    pps.getRolePrivileges().put("role1", Arrays.asList(new PrivilegeGrantInfo("exec", 1,
        "shaggy", PrincipalType.ROLE, true)));

    byte[] serialized = HBaseUtils.writePrivileges(pps);
    pps = HBaseUtils.readPrivileges(serialized);

    Assert.assertEquals(2, pps.getUserPrivileges().size());
    Assert.assertEquals(1, pps.getUserPrivileges().get("fred").size());
    PrivilegeGrantInfo pgi = pps.getUserPrivileges().get("fred").get(0);
    Assert.assertEquals("read", pgi.getPrivilege());
    Assert.assertEquals(1, pgi.getCreateTime());
    Assert.assertEquals("daphne", pgi.getGrantor());
    Assert.assertEquals(PrincipalType.USER, pgi.getGrantorType());
    Assert.assertTrue(pgi.isGrantOption());

    Assert.assertEquals(1, pps.getUserPrivileges().get("wilma").size());
    pgi = pps.getUserPrivileges().get("wilma").get(0);
    Assert.assertEquals("write", pgi.getPrivilege());
    Assert.assertEquals(1, pgi.getCreateTime());
    Assert.assertEquals("scooby", pgi.getGrantor());
    Assert.assertEquals(PrincipalType.USER, pgi.getGrantorType());
    Assert.assertFalse(pgi.isGrantOption());

    Assert.assertEquals(1, pps.getRolePrivileges().size());
    Assert.assertEquals(1, pps.getRolePrivileges().get("role1").size());
    pgi = pps.getRolePrivileges().get("role1").get(0);
    Assert.assertEquals("exec", pgi.getPrivilege());
    Assert.assertEquals(1, pgi.getCreateTime());
    Assert.assertEquals("shaggy", pgi.getGrantor());
    Assert.assertEquals(PrincipalType.ROLE, pgi.getGrantorType());
    Assert.assertTrue(pgi.isGrantOption());
  }
}
