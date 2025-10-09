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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
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

  @Override
  protected String getTestDataDir() {
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
    TableExtended(String name) {
      this(name, null);
    }
    TableExtended(String name, String partitionColumns) {
      this.name = name;
      this.partitionColumns = partitionColumns;
    }
  }

  @Override
  void initHiveConf() {
    super.initHiveConf();
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, false);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUpInternal();
    setUpInternalExtended(false);
  }
  
  void setUpInternalExtended(boolean isOrcFormat) throws Exception {
    hiveConf.setBoolVar(HiveConf.ConfVars.DYNAMIC_PARTITIONING, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_MAPRED_MODE, "nonstrict");
    hiveConf.setVar(HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
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

  @Override
  protected void dropTables() throws Exception {
    super.dropTables();
    for (TestTxnCommandsForMmTable.TableExtended t : TestTxnCommandsForMmTable.TableExtended.values()) {
      runStatementOnDriver("drop table if exists " + t);
    }
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
      status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
      status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
      status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
      status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
      status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
      status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
        (TableExtended.MMTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 delta dirs in the location
    Assert.assertEquals(2, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("delta_.*"));
    }

    // 2. Insert Overwrite.
    int[][] values = {{1,2},{2,4},{5,6},{6,8},{9,10}};
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + makeValuesClause(values));

    runStatementOnDriver("insert overwrite table " + TableExtended.MMTBL + " select a,b from " + Table.NONACIDORCTBL + " where a between 1 and 3 union all select a,b from " + Table.NONACIDORCTBL + " where a between 5 and 7");

    // Verify resulting dirs.
    boolean sawBase = false;
    String baseDir = "";
    int deltaCount = 0;

    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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
    status = fs.listStatus(new Path(getWarehouseDir() + "/" +
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

    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from COMPLETED_TXN_COMPONENTS"),
            2, TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPLETED_TXN_COMPONENTS"));

    // Initiate a major compaction request on the table.
    runStatementOnDriver("alter table " + TableExtended.MMTBL  + " compact 'MAJOR'");

    // Run worker.
    runWorker(hiveConf);
    verifyDirAndResult(2, true);

    // Run Cleaner.
    runCleaner(hiveConf);
    Assert.assertEquals(TestTxnDbUtil.queryToString(hiveConf, "select * from COMPLETED_TXN_COMPONENTS"),
            0,
            TestTxnDbUtil.countQueryAgent(hiveConf, "select count(*) from COMPLETED_TXN_COMPONENTS"));
    verifyDirAndResult(0, true);
  }

  @Test
  public void testImpalaTruncatedMmTableVectorized() throws Exception {
    testImpalaTruncatedMmTable(true);
  }

  @Test
  public void testImpalaTruncatedMmTableNonVectorized() throws Exception {
    testImpalaTruncatedMmTable(false);
  }

  /**
   * Impala truncates insert-only tables by writing a base directory (like insert overwrite) containing a completely
   * empty file. Make sure that Hive reads these bases correctly.
   *
   * @throws Exception
   */
  private void testImpalaTruncatedMmTable(boolean vectorized) throws Exception {
    if (!vectorized) {
      d.getConf().setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    }
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;
    Path tblLocation = new Path(getWarehouseDir() + "/" +
        (TableExtended.MMTBL).toString().toLowerCase());

    // 1. Insert two rows to an MM table
    runStatementOnDriver("drop table " + TableExtended.MMTBL);
    runStatementOnDriver("create table " + TableExtended.MMTBL + "(a int,b int) stored as parquet "
        + "TBLPROPERTIES ('transactional'='true', 'transactional_properties'='insert_only')");
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(1,2)");
    runStatementOnDriver("insert into " + TableExtended.MMTBL + "(a,b) values(3,4)");
    status = fs.listStatus(tblLocation, FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 delta dirs in the location
    Assert.assertEquals(2, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("delta_.*"));
    }

    // 2. Simulate Impala truncating the table: write a base dir (base_0000003) containing a file with no data. We
    // have to delete this file (it's not completely empty, it contains metadata) and create completely empty file
    runStatementOnDriver("insert overwrite  table " + TableExtended.MMTBL + " select * from "
        + TableExtended.MMTBL + " where 1=2");
    status = fs.listStatus(tblLocation, FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 delta dirs, plus 1 base dir in the location
    Assert.assertEquals(3, status.length);
    verifyDir(2, true);
    Path basePath = new Path(tblLocation, "base_0000003");
    Assert.assertTrue("Deleting file under base failed", fs.delete(new Path(basePath, "000000_0")));
    fs.create(new Path(basePath, "empty"));

    // 3. Verify query result. Selecting from a truncated table should return nothing.
    List<String> rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(Collections.emptyList(), rs);

    // 4. Perform a major compaction. Cleaner should remove the 2 delta dirs.
    runStatementOnDriver("alter table "+ TableExtended.MMTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    runCleaner(hiveConf);
    verifyDir(0, true);
    rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(Collections.emptyList(), rs);
    if (!vectorized) {
      d.getConf().setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    }
  }

  private void verifyDirAndResult(int expectedDeltas) throws Exception {
    verifyDirAndResult(expectedDeltas, false);
  }
  private void verifyDirAndResult(int expectedDeltas, boolean expectBaseDir) throws Exception {
    verifyDir(expectedDeltas, expectBaseDir);

    // Verify query result
    int [][] resultData = new int[][] {{1,2}, {3,4}};
    List<String> rs = runStatementOnDriver("select a,b from " + TableExtended.MMTBL + " order by a,b");
    Assert.assertEquals(stringifyValues(resultData), rs);
  }

  private void verifyDir(int expectedDeltas, boolean expectBaseDir) throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    // Verify the content of subdirs
    FileStatus[] status = fs.listStatus(new Path(getWarehouseDir() + "/" +
        (TableExtended.MMTBL).toString().toLowerCase()), FileUtils.HIDDEN_FILES_PATH_FILTER);
    int sawDeltaTimes = 0;
    int sawBaseTimes = 0;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("delta_.*")) {
        sawDeltaTimes++;
        FileStatus[] files = fs.listStatus(status[i].getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        Assert.assertEquals(1, files.length);
        Assert.assertEquals("000000_0", files[0].getPath().getName());
      } else {
        sawBaseTimes++;
      }
    }

    Assert.assertEquals(expectedDeltas, sawDeltaTimes);
    if (expectBaseDir) {
      Assert.assertEquals("1 base directory expected", 1, sawBaseTimes);
    } else {
      Assert.assertEquals("0 base directories expected", 0, sawBaseTimes);
    }
  }

  @Test
  public void testTruncateWithBase() throws Exception {
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, true);

    runStatementOnDriver("insert into " + TableExtended.MMTBL + " values(1,2),(3,4)");
    runStatementOnDriver("truncate table " + TableExtended.MMTBL);

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat =
        fs.listStatus(new Path(getWarehouseDir(), TableExtended.MMTBL.toString().toLowerCase()), AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000002", name);

    List<String> r = runStatementOnDriver("select * from " + TableExtended.MMTBL);
    Assert.assertEquals(0, r.size());
  }

  @Test
  public void testTruncateWithBaseAllPartition() throws Exception {
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, true);

    runStatementOnDriver("insert into " + TableExtended.MMTBLPART + " partition(p='a') values(1,2),(3,4)");
    runStatementOnDriver("insert into " + TableExtended.MMTBLPART + " partition(p='b') values(1,2),(3,4)");
    runStatementOnDriver("truncate table " + TableExtended.MMTBLPART);

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat =
        fs.listStatus(new Path(getWarehouseDir(), TableExtended.MMTBLPART.toString().toLowerCase() + "/p=a"), AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000003", name);

    List<String> r = runStatementOnDriver("select * from " + TableExtended.MMTBLPART);
    Assert.assertEquals(0, r.size());
  }

  @Test
  public void testTruncateWithBaseOnePartition() throws Exception {
    HiveConf.setBoolVar(hiveConf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE, true);

    runStatementOnDriver("insert into " + TableExtended.MMTBLPART + " partition(p='a') values(1,2),(3,4)");
    runStatementOnDriver("insert into " + TableExtended.MMTBLPART+ " partition(p='b') values(5,5),(4,4)");
    runStatementOnDriver("truncate table " + TableExtended.MMTBLPART + " partition(p='b')");

    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] stat =
        fs.listStatus(new Path(getWarehouseDir(), TableExtended.MMTBLPART.toString().toLowerCase() + "/p=b"), AcidUtils.baseFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 base and found " + stat.length + " files " + Arrays.toString(stat));
    }
    String name = stat[0].getPath().getName();
    Assert.assertEquals("base_0000003", name);
    stat =
        fs.listStatus(new Path(getWarehouseDir(), TableExtended.MMTBLPART.toString().toLowerCase() + "/p=a"), AcidUtils.deltaFileFilter);
    if (1 != stat.length) {
      Assert.fail("Expecting 1 delta and found " + stat.length + " files " + Arrays.toString(stat));
    }

    List<String> r = runStatementOnDriver("select * from " + TableExtended.MMTBLPART);
    Assert.assertEquals(2, r.size());
  }
}
