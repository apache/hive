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
package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestAlterTableMetadata {
  @Test
  public void testAlterTableOwner() throws HiveException, CommandProcessorException {
    /*
     * This test verifies that the ALTER TABLE ... SET OWNER command will change the
     * owner metadata of the table in HMS.
     */

    HiveConf conf = new HiveConf(this.getClass());
    conf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    SessionState.start(conf);
    IDriver driver = DriverFactory.newDriver(conf);
    Table table;

    driver.run("create table t1(id int)");

    // Changes the owner to a user and verify the change
    driver.run("alter table t1 set owner user u1");

    table = Hive.get(conf).getTable("t1");
    assertEquals(PrincipalType.USER, table.getOwnerType());
    assertEquals("u1", table.getOwner());

    // Changes the owner to a group and verify the change
    driver.run("alter table t1 set owner group g1");

    table = Hive.get(conf).getTable("t1");
    assertEquals(PrincipalType.GROUP, table.getOwnerType());
    assertEquals("g1", table.getOwner());

    // Changes the owner to a role and verify the change
    driver.run("alter table t1 set owner role r1");

    table = Hive.get(conf).getTable("t1");
    assertEquals(PrincipalType.ROLE, table.getOwnerType());
    assertEquals("r1", table.getOwner());

  }
}
