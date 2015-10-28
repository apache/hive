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
package org.apache.hadoop.hive.metastore;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;

public class TestAdminUser extends TestCase{

 public void testCreateAdminNAddUser() throws IOException, Throwable {
   HiveConf conf = new HiveConf();
   conf.setVar(ConfVars.USERS_IN_ADMIN_ROLE, "adminuser");
   conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,SQLStdHiveAuthorizerFactory.class.getName());
   RawStore rawStore = new HMSHandler("testcreateroot", conf).getMS();
   Role adminRole = rawStore.getRole(HiveMetaStore.ADMIN);
   assertTrue(adminRole.getOwnerName().equals(HiveMetaStore.ADMIN));
   assertEquals(rawStore.listPrincipalGlobalGrants(HiveMetaStore.ADMIN, PrincipalType.ROLE)
    .get(0).getGrantInfo().getPrivilege(),"All");
   assertEquals(rawStore.listRoles("adminuser", PrincipalType.USER).get(0).
     getRoleName(),HiveMetaStore.ADMIN);
 }
}