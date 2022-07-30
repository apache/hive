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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.execSelectAndDumpData;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriverSilently;

public class TestMaterializedViewRebuild extends CompactorOnTezTest {

  private static final String TABLE1 = "t1";
  private static final String MV1 = "mat1";

  private static final List<String> FULL_REBUILD_PLAN = Arrays.asList(
          "CBO PLAN:",
          "HiveProject(a=[$0], b=[$1], c=[$2])",
          "  HiveFilter(condition=[OR(IS NULL($0), >($0, 0))])",
          "    HiveTableScan(table=[[default, t1]], table:alias=[t1])",
          ""
  );

  private static final List<String> INCREMENTAL_REBUILD_PLAN = Arrays.asList(
          "CBO PLAN:",
          "HiveProject(a=[$0], b=[$1], c=[$2])",
          "  HiveFilter(condition=[AND(<(2, $5.writeid), OR(>($0, 0), IS NULL($0)))])",
          "    HiveTableScan(table=[[default, t1]], table:alias=[t1])",
          ""
  );

  private static final List<String> EXPECTED_RESULT = Arrays.asList(
          "1\tone\t1.1",
          "2\ttwo\t2.2",
          "3\tthree\t3.3",
          "NULL\tNULL\tNULL"
  );


  @Override
  public void setup() throws Exception {
    super.setup();

    executeStatementOnDriver("create table " + TABLE1 + "(a int, b varchar(128), c float) stored as orc TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + TABLE1 + "(a,b, c) values (1, 'one', 1.1), (2, 'two', 2.2), (NULL, NULL, NULL)", driver);
    executeStatementOnDriver("create materialized view " + MV1 + " stored as orc TBLPROPERTIES ('transactional'='true') as " +
            "select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver);
  }

  @Override
  public void tearDown() {
    executeStatementOnDriverSilently("drop materialized view " + MV1, driver);
    executeStatementOnDriverSilently("drop table " + TABLE1 , driver);

    super.tearDown();
  }

  @Test
  public void testWhenMajorCompactionThenIncrementalMVRebuildNotUsed() throws Exception {

    executeStatementOnDriver("insert into " + TABLE1 + "(a,b,c) values (3, 'three', 3.3)", driver);

    CompactorTestUtil.runCompaction(conf, "default",  TABLE1 , CompactionType.MAJOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessfulCompaction(1);
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.cleanTxnToWriteIdTable();

    List<String> result = execSelectAndDumpData("explain cbo alter materialized view " + MV1 + " rebuild", driver, "");
    Assert.assertEquals(FULL_REBUILD_PLAN, result);
    executeStatementOnDriver("alter materialized view " + MV1 + " rebuild", driver);

    result = execSelectAndDumpData("select * from " + MV1 , driver, "");
    assertResult(EXPECTED_RESULT, result);

    result = execSelectAndDumpData("explain cbo select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver, "");
    Assert.assertEquals(Arrays.asList("CBO PLAN:", "HiveTableScan(table=[[default, " + MV1 + "]], table:alias=[default." + MV1 + "])", ""), result);
  }

  @Test
  public void testSecondRebuildCanBeIncrementalAfterMajorCompaction() throws Exception {

    executeStatementOnDriver("insert into " + TABLE1 + "(a,b,c) values (3, 'three', 3.3)", driver);

    CompactorTestUtil.runCompaction(conf, "default",  TABLE1 , CompactionType.MAJOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessfulCompaction(1);
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.cleanTxnToWriteIdTable();

    executeStatementOnDriver("alter materialized view " + MV1 + " rebuild", driver);

    // Insert after first rebuild.
    executeStatementOnDriver("insert into " + TABLE1 + "(a,b,c) values (4, 'four', 4.4)", driver);

    List<String> result = execSelectAndDumpData("explain cbo alter materialized view " + MV1 + " rebuild", driver, "");
    Assert.assertEquals(INCREMENTAL_REBUILD_PLAN, result);
    executeStatementOnDriver("alter materialized view " + MV1 + " rebuild", driver);

    result = execSelectAndDumpData("select * from " + MV1 , driver, "");
    List<String> expected = new ArrayList(EXPECTED_RESULT);
    expected.add("4\tfour\t4.4");
    assertResult(expected, result);

    result = execSelectAndDumpData("explain cbo select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver, "");
    Assert.assertEquals(Arrays.asList("CBO PLAN:", "HiveTableScan(table=[[default, " + MV1 + "]], table:alias=[default." + MV1 + "])", ""), result);
  }

  @Test
  public void testWhenCleanUpOfMajorCompactionHasNotFinishedIncrementalMVRebuildNotUsed() throws Exception {
    executeStatementOnDriver("insert into " + TABLE1 + "(a,b,c) values (3, 'three', 3.3)", driver);

    CompactorTestUtil.runCompaction(conf, "default",  TABLE1 , CompactionType.MAJOR, true);

    List<String> result = execSelectAndDumpData("explain cbo alter materialized view " + MV1 + " rebuild", driver, "");
    Assert.assertEquals(FULL_REBUILD_PLAN, result);
    executeStatementOnDriver("alter materialized view " + MV1 + " rebuild", driver);

    result = execSelectAndDumpData("select * from " + MV1 , driver, "");
    assertResult(EXPECTED_RESULT, result);

    result = execSelectAndDumpData("explain cbo select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver, "");
    Assert.assertEquals(Arrays.asList("CBO PLAN:", "HiveTableScan(table=[[default, " + MV1 + "]], table:alias=[default." + MV1 + "])", ""), result);
  }

  private static final List<String> EXPECTED_RESULT_AFTER_UPDATE = Arrays.asList(
          "1\tChanged\t1.1",
          "2\ttwo\t2.2",
          "NULL\tNULL\tNULL"
  );

  @Test
  public void testWhenMajorCompactionThenIncrementalMVRebuildNotUsedInPresenceOfUpdate() throws Exception {
    executeStatementOnDriver("update " + TABLE1 + " set b = 'Changed' where a = 1", driver);

    CompactorTestUtil.runCompaction(conf, "default",  TABLE1 , CompactionType.MAJOR, true);
    CompactorTestUtil.runCleaner(conf);
    verifySuccessfulCompaction(1);
    TxnStore txnHandler = TxnUtils.getTxnStore(conf);
    txnHandler.cleanTxnToWriteIdTable();

    List<String> result = execSelectAndDumpData("explain cbo alter materialized view " + MV1 + " rebuild", driver, "");
    Assert.assertEquals(FULL_REBUILD_PLAN, result);
    executeStatementOnDriver("alter materialized view " + MV1 + " rebuild", driver);

    result = execSelectAndDumpData("select * from " + MV1 , driver, "");
    assertResult(EXPECTED_RESULT_AFTER_UPDATE, result);

    result = execSelectAndDumpData("explain cbo select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver, "");
    Assert.assertEquals(Arrays.asList("CBO PLAN:", "HiveTableScan(table=[[default, " + MV1 + "]], table:alias=[default." + MV1 + "])", ""), result);
  }

  private void assertResult(List<String> expected, List<String> actual) {
    Assert.assertEquals(expected.size(), actual.size());

    Collections.sort(expected);
    Collections.sort(actual);
    Assert.assertEquals(expected, actual);
  }

}
