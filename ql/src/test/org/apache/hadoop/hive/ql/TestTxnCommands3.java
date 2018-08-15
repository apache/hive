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
package org.apache.hadoop.hive.ql;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class TestTxnCommands3 extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnCommands3.class);
  private static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
      File.separator + TestTxnCommands3.class.getCanonicalName()
      + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  @Override
  protected String getTestDataDir() {
    return TEST_DATA_DIR;
  }
  @Test
  public void testRenameTable() throws Exception {
    MetastoreConf.setBoolVar(hiveConf, MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID, true);
    runStatementOnDriver("drop database if exists mydb1 cascade");
    runStatementOnDriver("drop database if exists mydb2 cascade");
    runStatementOnDriver("create database mydb1");
    runStatementOnDriver("create database mydb2");
    runStatementOnDriver("create table mydb1.T(a int, b int) stored as orc");
    runStatementOnDriver("insert into mydb1.T values(1,2),(4,5)");
    //put something in WRITE_SET
    runStatementOnDriver("update mydb1.T set b = 6 where b = 5");
    runStatementOnDriver("alter table mydb1.T compact 'minor'");

    runStatementOnDriver("alter table mydb1.T RENAME TO mydb1.S");

    String testQuery = "select ROW__ID, a, b, INPUT__FILE__NAME from mydb1.S";
    String[][] expected = new String[][] {
        {"{\"writeid\":1,\"bucketid\":536870912,\"rowid\":0}\t1\t2",
            "s/delta_0000001_0000001_0000/bucket_00000"},
        {"{\"writeid\":2,\"bucketid\":536870912,\"rowid\":0}\t4\t6",
            "s/delta_0000002_0000002_0000/bucket_00000"}};
    checkResult(expected, testQuery, false, "check data", LOG);


    Assert.assertEquals(0, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from COMPLETED_TXN_COMPONENTS where CTC_TABLE='t'"));
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from COMPACTION_QUEUE where CQ_TABLE='t'"));
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from WRITE_SET where WS_TABLE='t'"));
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE='t'"));
    Assert.assertEquals(0, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from NEXT_WRITE_ID where NWI_TABLE='t'"));

    Assert.assertEquals(
        TxnDbUtil.queryToString(hiveConf, "select * from COMPLETED_TXN_COMPONENTS"), 2,
        TxnDbUtil.countQueryAgent(hiveConf,
            "select count(*) from COMPLETED_TXN_COMPONENTS where CTC_TABLE='s'"));
    Assert.assertEquals(1, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from COMPACTION_QUEUE where CQ_TABLE='s'"));
    Assert.assertEquals(1, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from WRITE_SET where WS_TABLE='s'"));
    Assert.assertEquals(3, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE='s'"));
    Assert.assertEquals(1, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from NEXT_WRITE_ID where NWI_TABLE='s'"));

    runStatementOnDriver("alter table mydb1.S RENAME TO mydb2.bar");

    Assert.assertEquals(
        TxnDbUtil.queryToString(hiveConf, "select * from COMPLETED_TXN_COMPONENTS"), 2,
        TxnDbUtil.countQueryAgent(hiveConf,
            "select count(*) from COMPLETED_TXN_COMPONENTS where CTC_TABLE='bar'"));
    Assert.assertEquals(1, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from COMPACTION_QUEUE where CQ_TABLE='bar'"));
    Assert.assertEquals(1, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from WRITE_SET where WS_TABLE='bar'"));
    Assert.assertEquals(4, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from TXN_TO_WRITE_ID where T2W_TABLE='bar'"));
    Assert.assertEquals(1, TxnDbUtil.countQueryAgent(hiveConf,
        "select count(*) from NEXT_WRITE_ID where NWI_TABLE='bar'"));
  }

  /**
   * HIVE-19985
   */
  @Test
  public void testAcidMetaColumsDecode() throws Exception {
    //this only applies in vectorized mode
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    hiveConf.set(MetastoreConf.ConfVars.CREATE_TABLES_AS_ACID.getVarname(), "true");
    runStatementOnDriver("drop table if exists T");
    runStatementOnDriver("create table T (a int, b int) stored as orc");
    int[][] data1 = {{1,2}, {3,4}};
    runStatementOnDriver("insert into T" + makeValuesClause(data1));
    int[][] data2 = {{5,6}, {7,8}};
    runStatementOnDriver("insert into T" + makeValuesClause(data2));
    int[][] dataAll = {{1,2}, {3,4}, {5,6}, {7,8}};


    hiveConf.setBoolVar(HiveConf.ConfVars.OPTIMIZE_ACID_META_COLUMNS, true);
    List<String> rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(dataAll), rs);

    hiveConf.setBoolVar(HiveConf.ConfVars.OPTIMIZE_ACID_META_COLUMNS, false);
    rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(dataAll), rs);

    runStatementOnDriver("alter table T compact 'major'");
    TestTxnCommands2.runWorker(hiveConf);

    //check status of compaction job
    TxnStore txnHandler = TxnUtils.getTxnStore(hiveConf);
    ShowCompactResponse resp = txnHandler.showCompact(new ShowCompactRequest());
    Assert.assertEquals("Unexpected number of compactions in history", 1, resp.getCompactsSize());
    Assert.assertEquals("Unexpected 0 compaction state", TxnStore.CLEANING_RESPONSE, resp.getCompacts().get(0).getState());
    Assert.assertTrue(resp.getCompacts().get(0).getHadoopJobId().startsWith("job_local"));

    hiveConf.setBoolVar(HiveConf.ConfVars.OPTIMIZE_ACID_META_COLUMNS, true);
    rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(dataAll), rs);

    hiveConf.setBoolVar(HiveConf.ConfVars.OPTIMIZE_ACID_META_COLUMNS, false);
    rs = runStatementOnDriver("select a, b from T order by a, b");
    Assert.assertEquals(stringifyValues(dataAll), rs);
  }

}