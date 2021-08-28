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

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.txn.AcidCompactionHistoryService;
import org.apache.hadoop.hive.metastore.txn.TxnDbUtil;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.metastore.txn.AcidOpenTxnsCounterService;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.Initiator;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests here are for micro-managed tables:
 * specifically INSERT OVERWRITE statements and Major/Minor Compactions.
 */
public class TestTxnCommandsForMmTable extends TxnCommandsBaseForTests {
  static final private Logger LOG = LoggerFactory.getLogger(TestTxnCommandsForMmTable.class);
  protected static final String TEST_DATA_DIR = new File(System.getProperty("java.io.tmpdir") +
    File.separator + TestTxnCommands.class.getCanonicalName()
    + "-" + System.currentTimeMillis()
  ).getPath().replaceAll("\\\\", "/");
  protected static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  @Override
  String getTestDataDir() {
    return TEST_DATA_DIR;
  }

  enum TableExtended {
    NONACIDPART("nonAcidPart", "p"),
    MMTBL("mmTbl"),
    MMTBL2("mmTbl2"),
    MMTBLPART("mmTblPart","p");

    final String name;
    final String partitionColumns;
    @Override
    public String toString() {
      return name;
    }
    String getPartitionColumns() {
      return partitionColumns;
    }
    TableExtended(String name) {
      this(name, null);
    }
    TableExtended(String name, String partitionColumns) {
      this.name = name;
      this.partitionColumns = partitionColumns;
    }
  }
  
  @Override
  @Before
  public void setUp() throws Exception {
    super.setUpInternal();
    setUpInternalExtended(false);
  }
  
  void setUpInternalExtended(boolean isOrcFormat) throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.DYNAMICPARTITIONING, true);
    hiveConf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    hiveConf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    hiveConf.setVar(HiveConf.ConfVars.HIVEFETCHTASKCONVERSION, "none");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "true");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");

    runStatementOnDriver("create table " + TableExtended.NONACIDPART + "(a int, b int) partitioned by (p string) stored as orc TBLPROPERTIES ('transactional'='false')");
    if (!isOrcFormat) {
      runStatementOnDriver("create table " + TableExtended.MMTBL + "(a int, b int) TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
      runStatementOnDriver("create table " + TableExtended.MMTBL2 + "(a int, b int) TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
      runStatementOnDriver("create table " + TableExtended.MMTBLPART + "(a int, b int) partitioned by (p string) TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
    } else {
      runStatementOnDriver("create table " + TableExtended.MMTBL + "(a int, b int) stored as orc TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
      runStatementOnDriver("create table " + TableExtended.MMTBL2 + "(a int, b int) stored as orc TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
      runStatementOnDriver("create table " + TableExtended.MMTBLPART + "(a int, b int) partitioned by (p string) stored as orc TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
    }
  }
  protected void dropTables() throws Exception {
    super.dropTables();
    for(TestTxnCommandsForMmTable.TableExtended t : TestTxnCommandsForMmTable.TableExtended.values()) {
      runStatementOnDriver("drop table if exists " + t);
    }
  }
  /**
   * Test compaction for Micro-managed table
   * 1. Regular compaction shouldn't impact any valid subdirectories of MM tables
   * 2. Compactions will only remove subdirectories for aborted transactions of MM tables, if any
   * @throws Exception
   */
  @Test
  public void testMmTableCompaction() throws Exception {
    // 1. Insert some rows into MM table
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(3,4)");
    // There should be 2 delta directories
    verifyDirAndResult(2);

    // 2. Perform a MINOR compaction. Since nothing was aborted, subdirs should stay.
    runStatementOnDriver("alter table "+ TableExtended.MMTBL + " compact 'MINOR'");
    runWorker(hiveConf);
    verifyDirAndResult(2);

    // 3. Let a transaction be aborted
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, true);
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(5,6)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, false);
    // There should be 3 delta directories. The new one is the aborted one.
    verifyDirAndResult(3);

    // 4. Perform a MINOR compaction again. This time it will remove the subdir for aborted transaction.
    runStatementOnDriver("alter table "+ TableExtended.MMTBL + " compact 'MINOR'");
    runWorker(hiveConf);
    // The worker should remove the subdir for aborted transaction
    verifyDirAndResult(2);

    // 5. Run Cleaner. Shouldn't impact anything.
    runCleaner(hiveConf);
    verifyDirAndResult(2);
  }

  /**
   * Test a scenario, on a micro-managed table, where an IOW comes in
   * after a MAJOR compaction, and then a MINOR compaction is initiated.
   *
   * @throws Exception
   */
  @Test
  public void testInsertOverwriteForMmTable() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert two rows to an MM table
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(3,4)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (TableExtended.MMTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 delta dirs in the location
    Assert.assertEquals(2, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("delta_.*"));
    }

    // 2. INSERT OVERWRITE
    // Prepare data for the source table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(5,6),(7,8)");
    // Insert overwrite MM table from source table
    runStatementOnDriver("insert overwrite table " + TableExtended.MMTBL + " select a,b from " + Table.NONACIDORCTBL);
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (TableExtended.MMTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 delta dirs, plus 1 base dir in the location
    Assert.assertEquals(3, status.length);
    int baseCount = 0;
    int deltaCount = 0;
    for (int i = 0; i < status.length; i++) {
      String dirName = status[i].getPath().getName();
      if (dirName.matches("delta_.*")) {
        deltaCount++;
      } else {
        baseCount++;
      }
    }
    Assert.assertEquals(2, deltaCount);
    Assert.assertEquals(1, baseCount);

    // Verify query result
    int[][] resultData = new int[][] {{5,6},{7,8}};
    List<String> rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(resultData), rs);
  }

  /**
   * Test a scenario, on a partitioned micro-managed table, that an IOW comes in
   * before a MAJOR compaction happens.
   *
   * @throws Exception
   */
  @Test
  public void testInsertOverwriteForPartitionedMmTable() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert two rows to a partitioned MM table.
    int[][] valuesOdd = {{5,6},{7,8}};
    int[][] valuesEven = {{2,1},{4,3}};
    runStatementOnDriver("insert into " + TableExtended.MMTBLPART + " PARTITION(p='odd') " + makeValuesClause(valuesOdd));
    runStatementOnDriver("insert into " + TableExtended.MMTBLPART + " PARTITION(p='even') " + makeValuesClause(valuesEven));

    // Verify dirs
    String[] pStrings = {"/p=odd", "/p=even"};

    for(int i=0; i < pStrings.length; i++) {
      status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
          (TableExtended.MMTBLPART).toString().toLowerCase() + pStrings[i]), FileUtils.STAGING_DIR_PATH_FILTER);
      // There should be 1 delta dir per partition location
      Assert.assertEquals(1, status.length);
      Assert.assertTrue(status[0].getPath().getName().matches("delta_.*"));
    }

    // 2. INSERT OVERWRITE
    // Prepare data for the source table
    int[][] newValsOdd = {{5,5},{11,11}};
    int[][] newValsEven = {{2,2}};

    runStatementOnDriver("insert into " + TableExtended.NONACIDPART + " PARTITION(p='odd') " + makeValuesClause(newValsOdd));
    runStatementOnDriver("insert into " + TableExtended.NONACIDPART + " PARTITION(p='even') " + makeValuesClause(newValsEven));

    // Insert overwrite MM table from source table
    List<String> rs = null;
    String s = "insert overwrite table " + TableExtended.MMTBLPART + " PARTITION(p='odd') " +
      " select a,b from " + TableExtended.NONACIDPART + " where " + TableExtended.NONACIDPART + ".p='odd'";
    rs = runStatementOnDriver("explain formatted " + s);
    LOG.info("Explain formatted: " + rs.toString());
    runStatementOnDriver(s);

    s = "insert overwrite table " + TableExtended.MMTBLPART + " PARTITION(p='even') " +
        " select a,b from " + TableExtended.NONACIDPART + " where " + TableExtended.NONACIDPART + ".p='even'";
    runStatementOnDriver(s);

    // Verify resulting dirs.
    boolean sawBase = false;
    String[] baseDirs = {"", ""};
    int deltaCount = 0;
    for(int h=0; h < pStrings.length; h++) {
      status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
          (TableExtended.MMTBLPART).toString().toLowerCase() + pStrings[h]), FileUtils.STAGING_DIR_PATH_FILTER);
      // There should be 1 delta dir, plus a base dir in the location
      Assert.assertEquals(2, status.length);
      for (int i = 0; i < status.length; i++) {
        String dirName = status[i].getPath().getName();
        if (dirName.matches("delta_.*")) {
          deltaCount++;
        } else {
          sawBase = true;
          baseDirs[h] = dirName;
          Assert.assertTrue(baseDirs[h].matches("base_.*"));
        }
      }
      Assert.assertEquals(1, deltaCount);
      Assert.assertTrue(sawBase);
      deltaCount = 0;
      sawBase = false;
    }

    // Verify query result
    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBLPART + " where p='even' order by a,b");
    int [][] rExpectedEven = new int[][] {{2,2}};
    Assert.assertEquals(stringifyValues(rExpectedEven), rs);

    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBLPART + " where p='odd' order by a,b");
    int [][] rExpectedOdd  = new int[][] {{5,5},{11,11}};
    Assert.assertEquals(stringifyValues(rExpectedOdd), rs);

    // 3. Perform a major compaction. Nothing should change.
    // Both deltas and base dirs should have the same name.
    // Re-verify directory layout and query result by using the same logic as above
    runStatementOnDriver("alter table "+ TableExtended.MMTBLPART + " PARTITION(p='odd') " + " compact 'MAJOR'" );
    runWorker(hiveConf);
    runStatementOnDriver("alter table "+ TableExtended.MMTBLPART + " PARTITION(p='even') " + " compact 'MAJOR'" );
    runWorker(hiveConf);

    for(int h=0; h < pStrings.length; h++) {
      status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
          (TableExtended.MMTBLPART).toString().toLowerCase() + pStrings[h]), FileUtils.STAGING_DIR_PATH_FILTER);
      // There should be 2 delta dirs, plus a base dir in the location
      Assert.assertEquals(2, status.length);
      sawBase = false;
      deltaCount = 0;
      for (int i = 0; i < status.length; i++) {
        String dirName = status[i].getPath().getName();
        if (dirName.matches("delta_.*")) {
          deltaCount++;
        } else {
          sawBase = true;
          Assert.assertTrue("BASE ERROR: " + dirName, dirName.matches("base_.*"));
          Assert.assertEquals(baseDirs[h], dirName);
        }
      }
      Assert.assertEquals(1, deltaCount);
      Assert.assertTrue(sawBase);
      deltaCount = 0;
      sawBase = false;
    }

    // Verify query result
    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBLPART + " order by a,b");
    int[][] rExpected = new int[][] {{2,2},{5,5},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), rs);

    // 4. Run Cleaner. It should remove the 2 delta dirs.
    runCleaner(hiveConf);

    // There should be only 1 directory left: base_xxxxxxx.
    // The delta dirs should have been cleaned up.
    for(int h=0; h < pStrings.length; h++) {
      status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
          (TableExtended.MMTBLPART).toString().toLowerCase() + pStrings[h]), FileUtils.STAGING_DIR_PATH_FILTER);
      Assert.assertEquals(1, status.length);
      Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
      Assert.assertEquals(baseDirs[h], status[0].getPath().getName());
    }
    // Verify query result
    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBLPART + " order by a,b");
    Assert.assertEquals(stringifyValues(rExpected), rs);
  }

  /**
   * Test a scenario, on a dynamically partitioned micro-managed table, that an IOW comes in
   * before a MAJOR compaction happens.
   *
   * @throws Exception
   */
  @Test
  public void testInsertOverwriteWithDynamicPartition() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert two rows to a partitioned MM table.
    int[][] valuesOdd = {{5,6},{7,8}};
    int[][] valuesEven = {{2,1},{4,3}};
    runStatementOnDriver("insert into " + TableExtended.MMTBLPART + " PARTITION(p='odd') " + makeValuesClause(valuesOdd));
    runStatementOnDriver("insert into " + TableExtended.MMTBLPART + " PARTITION(p='even') " + makeValuesClause(valuesEven));

    // Verify dirs
    String[] pStrings = {"/p=odd", "/p=even"};

    for(int i=0; i < pStrings.length; i++) {
      status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
          (TableExtended.MMTBLPART).toString().toLowerCase() + pStrings[i]), FileUtils.STAGING_DIR_PATH_FILTER);
      // There should be 1 delta dir per partition location
      Assert.assertEquals(1, status.length);
      Assert.assertTrue(status[0].getPath().getName().matches("delta_.*"));
    }

    // 2. INSERT OVERWRITE
    // Prepare data for the source table
    int[][] newValsOdd = {{5,5},{11,11}};
    int[][] newValsEven = {{2,2}};

    runStatementOnDriver("insert into " + TableExtended.NONACIDPART + " PARTITION(p='odd') " + makeValuesClause(newValsOdd));
    runStatementOnDriver("insert into " + TableExtended.NONACIDPART + " PARTITION(p='even') " + makeValuesClause(newValsEven));

    runStatementOnDriver("insert overwrite table " + TableExtended.MMTBLPART + " partition(p) select a,b,p from " + TableExtended.NONACIDPART);

    // Verify resulting dirs.
    boolean sawBase = false;
    String[] baseDirs = {"", ""};
    int deltaCount = 0;
    for(int h=0; h < pStrings.length; h++) {
      status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
          (TableExtended.MMTBLPART).toString().toLowerCase() + pStrings[h]), FileUtils.STAGING_DIR_PATH_FILTER);
      // There should be 1 delta dir, plus a base dir in the location
      Assert.assertEquals(2, status.length);   // steve

      for (int i = 0; i < status.length; i++) {
        String dirName = status[i].getPath().getName();
        if (dirName.matches("delta_.*")) {
          deltaCount++;
        } else {
          sawBase = true;
          baseDirs[h] = dirName;
          Assert.assertTrue(baseDirs[h].matches("base_.*"));
        }
      }
      Assert.assertEquals(1, deltaCount);
      Assert.assertTrue(sawBase);
      deltaCount = 0;
      sawBase = false;
    }

    // Verify query result
    List<String> rs = null;
    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBLPART + " where p='even' order by a,b");
    int [][] rExpectedEven = new int[][] {{2,2}};
    Assert.assertEquals(stringifyValues(rExpectedEven), rs);

    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBLPART + " where p='odd' order by a,b");
    int [][] rExpectedOdd  = new int[][] {{5,5},{11,11}};
    Assert.assertEquals(stringifyValues(rExpectedOdd), rs);

    // Verify query result
    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBLPART + " order by a,b");
    int[][] rExpected = new int[][] {{2,2},{5,5},{11,11}};
    Assert.assertEquals(stringifyValues(rExpected), rs);
  }

  @Test
  public void testInsertOverwriteWithUnionAll() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert two rows to an MM table
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(3,4)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (TableExtended.MMTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 delta dirs in the location
    Assert.assertEquals(2, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("delta_.*"));
    }

    // 2. Insert Overwrite.
    int[][] values = {{1,2},{2,4},{5,6},{6,8},{9,10}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + TestTxnCommands2.makeValuesClause(values));

    runStatementOnDriver("insert overwrite table " + TableExtended.MMTBL + " select a,b from " + Table.NONACIDORCTBL + " where a between 1 and 3 union all select a,b from " + Table.NONACIDORCTBL + " where a between 5 and 7");

    // Verify resulting dirs.
    boolean sawBase = false;
    String baseDir = "";
    int deltaCount = 0;

    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (TableExtended.MMTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 delta dirs, plus a base dir in the location
    Assert.assertEquals(3, status.length);

    for (int i = 0; i < status.length; i++) {
      String dirName = status[i].getPath().getName();
      if (dirName.matches("delta_.*")) {
        deltaCount++;
      } else {
        sawBase = true;
        baseDir = dirName;
        Assert.assertTrue(baseDir.matches("base_.*"));
      }
    }
    Assert.assertEquals(2, deltaCount);
    Assert.assertTrue(sawBase);

    List<String> rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    int[][] rExpected = new int[][] {{1,2},{2,4},{5,6},{6,8}};
    Assert.assertEquals(stringifyValues(rExpected), rs);

    // 4. Perform a major compaction.
    runStatementOnDriver("alter table "+ TableExtended.MMTBL + " compact 'MAJOR'");
    runWorker(hiveConf);

    // 5. Run Cleaner. It should remove the 2 delta dirs.
    runCleaner(hiveConf);

    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(rExpected), rs);

    // Verify resulting dirs.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
      (TableExtended.MMTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be one base dir in the location
    Assert.assertEquals(1, status.length);

    sawBase = false;
    deltaCount = 0;
    for (int i = 0; i < status.length; i++) {
      String dirName = status[i].getPath().getName();
      if (dirName.matches("delta_.*")) {
        deltaCount++;
      } else {
        sawBase = true;
        baseDir = dirName;
        Assert.assertTrue(baseDir.matches("base_.*"));
      }
    }
    Assert.assertEquals(0, deltaCount);
    Assert.assertTrue(sawBase);

    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(rExpected), rs);
  }

  @Test
  public void testOperationsOnCompletedTxnComponentsForMmTable() throws Exception {

    // Insert two rows into the table.
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(3,4)");
    // There should be 2 delta directories
    verifyDirAndResult(2);

    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from COMPLETED_TXN_COMPONENTS"),
            2, TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPLETED_TXN_COMPONENTS"));
    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from TXNS"),
            0, TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXNS"));

    // Initiate a minor compaction request on the table.
    runStatementOnDriver("alter table " + TableExtended.MMTBL  + " compact 'MAJOR'");

    // Run worker.
    runWorker(hiveConf);

    // Run Cleaner.
    runCleaner(hiveConf);
    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from COMPLETED_TXN_COMPONENTS"),
            0,
            TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPLETED_TXN_COMPONENTS"));
    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from TXNS"),
            0, TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXNS"));
  }

  @Test
  public void testSnapshotIsolationWithAbortedTxnOnMmTable() throws Exception {

    // Insert two rows into the table.
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(3,4)");
    // There should be 2 delta directories
    verifyDirAndResult(2);

    // Initiate a minor compaction request on the table.
    runStatementOnDriver("alter table " + TableExtended.MMTBL  + " compact 'MINOR'");

    // Run Compaction Worker to do compaction.
    // But we do not compact a MM table but only transit the compaction request to
    // "ready for cleaning" state in this case.
    runWorker(hiveConf);
    verifyDirAndResult(2);

    // Start an INSERT statement transaction and roll back this transaction.
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, true);
    runStatementOnDriver("insert into " + TableExtended.MMTBL  + " values (5, 6)");
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVETESTMODEROLLBACKTXN, false);
    // There should be 3 delta directories. The new one is the aborted one.
    verifyDirAndResult(3);

    // Execute SELECT statement and verify the result set (should be two rows).
    int[][] expected = new int[][] {{1, 2}, {3, 4}};
    List<String> rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(expected), rs);

    // Run Cleaner.
    // This run doesn't do anything for the above aborted transaction since
    // the current compaction request entry in the compaction queue is updated
    // to have highest_write_id when the worker is run before the aborted
    // transaction. Specifically the id is 2 for the entry but the aborted
    // transaction has 3 as writeId. This run does transition the entry
    // "successful".
    runCleaner(hiveConf);
    verifyDirAndResult(3);

    // Execute SELECT and verify that aborted operation is not counted for MM table.
    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(expected), rs);

    // Run initiator to execute CompactionTxnHandler.cleanEmptyAbortedTxns()
    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from TXNS"),
            1, TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXNS"));
    Initiator i = new Initiator();
    i.setThreadId((int)i.getId());
    i.setConf(hiveConf);
    AtomicBoolean stop = new AtomicBoolean(true);
    i.init(stop, new AtomicBoolean());
    i.run();
    // This run of Initiator doesn't add any compaction_queue entry
    // since we only have one MM table with data - we don't compact MM tables.
    verifyDirAndResult(3);
    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from TXNS"),
            1, TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXNS"));

    // Execute SELECT statement and verify that aborted INSERT statement is not counted.
    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(expected), rs);

    // Initiate a minor compaction request on the table.
    runStatementOnDriver("alter table " + TableExtended.MMTBL  + " compact 'MINOR'");

    // Run worker to delete aborted transaction's delta directory.
    runWorker(hiveConf);
    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from TXNS"),
            1, TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXNS"));
    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from TXN_COMPONENTS"),
            1,
            TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS"));
    verifyDirAndResult(2);

    // Run Cleaner to delete rows for the aborted transaction
    // from TXN_COMPONENTS.
    runCleaner(hiveConf);

    // Run initiator to clean the row fro the aborted transaction from TXNS.
    i.run();
    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from TXNS"),
            0, TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXNS"));
    Assert.assertEquals(TxnDbUtil.queryToString(hiveConf, "select * from TXN_COMPONENTS"),
            0,
            TxnDbUtil.countQueryAgent(hiveConf, "select count(*) from TXN_COMPONENTS"));
  }

  private void verifyDirAndResult(int expectedDeltas) throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    // Verify the content of subdirs
    FileStatus[] status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (TableExtended.MMTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    int sawDeltaTimes = 0;
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("delta_.*"));
      sawDeltaTimes++;
      FileStatus[] files = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
      Assert.assertEquals(1, files.length);
      Assert.assertTrue(files[0].getPath().getName().equals("000000_0"));
    }
    Assert.assertEquals(expectedDeltas, sawDeltaTimes);

    // Verify query result
    int [][] resultData = new int[][] {{1,2}, {3,4}};
    List<String> rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(resultData), rs);
  }
}
