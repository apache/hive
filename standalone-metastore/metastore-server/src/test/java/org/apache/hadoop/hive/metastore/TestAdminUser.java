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
package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestAdminUser {

  @Test
  public void testCreateAdminNAddUser() throws MetaException, NoSuchObjectException {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setVar(conf, ConfVars.USERS_IN_ADMIN_ROLE, "adminuser");
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    HMSHandler hms = new HMSHandler("testcreateroot", conf);
    hms.init();
    RawStore rawStore = hms.getMS();
    Role adminRole = rawStore.getRole(HMSHandler.ADMIN);
    Assert.assertTrue(adminRole.getOwnerName().equals(HMSHandler.ADMIN));
    Assert.assertEquals(rawStore.listPrincipalGlobalGrants(HMSHandler.ADMIN, PrincipalType.ROLE)
     .get(0).getGrantInfo().getPrivilege(),"All");
    Assert.assertEquals(rawStore.listRoles("adminuser", PrincipalType.USER).get(0).
      getRoleName(),HMSHandler.ADMIN);
 }
}