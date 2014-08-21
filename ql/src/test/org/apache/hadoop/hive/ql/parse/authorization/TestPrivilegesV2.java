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
package org.apache.hadoop.hive.ql.parse.authorization;

import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class TestPrivilegesV2 extends PrivilegesTestBase{

  private HiveConf conf;
  private Hive db;
  private Table table;
  private Partition partition;

  @Before
  public void setup() throws Exception {
    conf = new HiveConf();
    //set authorization mode to V2
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        SQLStdHiveAuthorizerFactory.class.getName());
    db = Mockito.mock(Hive.class);
    table = new Table(DB, TABLE);
    SessionState.start(conf);
    Mockito.when(db.getTable(DB, TABLE, false)).thenReturn(table);
    Mockito.when(db.getTable(TABLE_QNAME, false)).thenReturn(table);
    Mockito.when(db.getPartition(table, new HashMap<String, String>(), false))
    .thenReturn(partition);
  }

  /**
   * Check acceptable privileges in grant statement
   * @return
   * @throws Exception
   */
  @Test
  public void testPrivInGrant() throws Exception{
    grantUserTable("select", PrivilegeType.SELECT);
    grantUserTable("insert", PrivilegeType.INSERT);
    grantUserTable("delete", PrivilegeType.DELETE);
    grantUserTable("update", PrivilegeType.ALTER_DATA);
    grantUserTable("all", PrivilegeType.ALL);
  }

  private void grantUserTable(String privName, PrivilegeType privType) throws Exception {
    grantUserTable(privName, privType, conf, db);
  }

}
