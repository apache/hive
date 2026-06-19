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
package org.apache.hadoop.hive.ql.parse;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * TestReplScenariosWithStrictManaged - Test all replication scenarios with strict managed enabled
 * at source and target.
 */
public class TestReplScenariosWithStrictManaged extends BaseReplicationAcrossInstances {

  @BeforeClass
  public static void classLevelSetup() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put(HiveConf.ConfVars.HIVE_STRICT_MANAGED_TABLES.varname, "true");
    overrides.put(MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID.getHiveName(), "true");
    overrides.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "true");
    overrides.put(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");

    internalBeforeClassSetup(overrides, TestReplScenariosWithStrictManaged.class);
  }

  @Test
  public void dynamicallyConvertManagedToExternalTable() throws Throwable {
    // All tables are automatically converted to ACID tables when strict managed is enabled.
    // Also, it is not possible to convert ACID table to external table.
    primary.run("use " + primaryDbName)
            .run("create table t1 (id int) stored as orc")
            .run("insert into table t1 values (1)")
            .run("create table t2 (id int) partitioned by (key int) stored as orc")
            .run("insert into table t2 partition(key=10) values (1)")
            .runFailure("alter table t1 set tblproperties('EXTERNAL'='true')")
            .runFailure("alter table t1 set tblproperties('EXTERNAL'='true', 'TRANSACTIONAL'='false')")
            .runFailure("alter table t2 set tblproperties('EXTERNAL'='true')");
  }

  @Test
  public void dynamicallyConvertExternalToManagedTable() throws Throwable {
    // With Strict managed enabled, it is not possible to convert external table to ACID table.
    primary.run("use " + primaryDbName)
            .run("create external table t1 (id int) stored as orc")
            .run("insert into table t1 values (1)")
            .run("create external table t2 (place string) partitioned by (country string)")
            .run("insert into table t2 partition(country='india') values ('bangalore')")
            .runFailure("alter table t1 set tblproperties('EXTERNAL'='false')")
            .runFailure("alter table t1 set tblproperties('EXTERNAL'='false', 'TRANSACTIONAL'='true')")
            .runFailure("alter table t2 set tblproperties('EXTERNAL'='false')");
  }
}
