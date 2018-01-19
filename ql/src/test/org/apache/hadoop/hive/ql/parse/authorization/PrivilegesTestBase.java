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
package org.apache.hadoop.hive.ql.parse.authorization;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.plan.DDLWork;
import org.apache.hadoop.hive.ql.plan.GrantDesc;
import org.apache.hadoop.hive.ql.plan.PrincipalDesc;
import org.apache.hadoop.hive.ql.plan.PrivilegeDesc;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeType;

public class PrivilegesTestBase {
  protected static final String DB = "default";
  protected static final String TABLE = "table1";
  protected static final String TABLE_QNAME = DB + "." + TABLE;
  protected static final String USER = "user1";

  public static void grantUserTable(String privStr, PrivilegeType privType, QueryState queryState, Hive db)
      throws Exception {
    DDLWork work = AuthorizationTestUtil.analyze("GRANT " + privStr + " ON TABLE " + TABLE + " TO USER " + USER, queryState, db);
    GrantDesc grantDesc = work.getGrantDesc();
    Assert.assertNotNull("Grant should not be null", grantDesc);

    //check privileges
    for(PrivilegeDesc privilege : ListSizeMatcher.inList(grantDesc.getPrivileges()).ofSize(1)) {
      Assert.assertEquals(privType, privilege.getPrivilege().getPriv());
    }

    //check other parts
    for(PrincipalDesc principal : ListSizeMatcher.inList(grantDesc.getPrincipals()).ofSize(1)) {
      Assert.assertEquals(PrincipalType.USER, principal.getType());
      Assert.assertEquals(USER, principal.getName());
    }
    Assert.assertTrue("Expected table", grantDesc.getPrivilegeSubjectDesc().getTable());
    Assert.assertEquals(TABLE_QNAME, grantDesc.getPrivilegeSubjectDesc().getObject());
  }

}
