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
package org.apache.hadoop.hive.ql.txn.compactor;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.execSelectAndDumpData;

public class TestMaterializedViewRebuild extends CompactorOnTezTest {
  @Test
  public void testIncrementalMVRebuild() throws Exception {
    System.out.println("hive.metastore.warehouse.dir:\n" + conf.get("hive.metastore.warehouse.dir"));


    executeStatementOnDriver("create table t1(a int, b varchar(128), c float) stored as orc TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into t1(a,b, c) values (1, 'one', 1.1), (2, 'two', 2.2), (NULL, NULL, NULL)", driver);
    executeStatementOnDriver("create materialized view mat1 stored as orc TBLPROPERTIES ('transactional'='true') as " +
        "select a,b,c from t1 where a > 0 or a is null", driver);

    executeStatementOnDriver("delete from t1 where a = 1", driver);

    CompactorTestUtil.runCompaction(conf, "default", "t1", CompactionType.MAJOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessfulCompaction(1);

    executeStatementOnDriver("alter materialized view mat1 rebuild", driver);

    List<String> result = execSelectAndDumpData("select * from mat1", driver, "");
    Assert.assertEquals(Arrays.asList("2\ttwo\t2.2", "NULL\tNULL\tNULL"), result);

    result = execSelectAndDumpData("explain cbo select a,b,c from t1 where a > 0 or a is null", driver, "");
    Assert.assertEquals(Arrays.asList("CBO PLAN:", "HiveTableScan(table=[[default, mat1]], table:alias=[default.mat1])", ""), result);
  }

  @Test
  public void testIncrementalMVRebuildIfCleanUpHasNotFinished() throws Exception {
    System.out.println("hive.metastore.warehouse.dir:\n" + conf.get("hive.metastore.warehouse.dir"));


    executeStatementOnDriver("create table t1(a int, b varchar(128), c float) stored as orc TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into t1(a,b, c) values (1, 'one', 1.1), (2, 'two', 2.2), (NULL, NULL, NULL)", driver);
    executeStatementOnDriver("create materialized view mat1 stored as orc TBLPROPERTIES ('transactional'='true') as " +
        "select a,b,c from t1 where a > 0 or a is null", driver);

    executeStatementOnDriver("delete from t1 where a = 1", driver);

    CompactorTestUtil.runCompaction(conf, "default", "t1", CompactionType.MAJOR, true);

    executeStatementOnDriver("alter materialized view mat1 rebuild", driver);

    List<String> result = execSelectAndDumpData("select * from mat1", driver, "");
    Assert.assertEquals(Arrays.asList("2\ttwo\t2.2", "NULL\tNULL\tNULL"), result);

    result = execSelectAndDumpData("explain cbo select a,b,c from t1 where a > 0 or a is null", driver, "");
    Assert.assertEquals(Arrays.asList("CBO PLAN:", "HiveTableScan(table=[[default, mat1]], table:alias=[default.mat1])", ""), result);
  }

}
