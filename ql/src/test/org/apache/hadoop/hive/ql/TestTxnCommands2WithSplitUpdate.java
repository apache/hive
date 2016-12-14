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

package org.apache.hadoop.hive.ql;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Same as TestTxnCommands2 but tests ACID tables with 'transactional_properties' set to 'default'.
 * This tests whether ACID tables with split-update turned on are working correctly or not
 * for the same set of tests when it is turned off. Of course, it also adds a few tests to test
 * specific behaviors of ACID tables with split-update turned on.
 */
public class TestTxnCommands2WithSplitUpdate extends TestTxnCommands2 {

  public TestTxnCommands2WithSplitUpdate() {
    super();
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Override
  @Before
  public void setUp() throws Exception {
    setUpWithTableProperties("'transactional'='true','transactional_properties'='default'");
  }

  @Override
  @Test
  public void testInitiatorWithMultipleFailedCompactions() throws Exception {
    // Test with split-update turned on.
    testInitiatorWithMultipleFailedCompactionsForVariousTblProperties("'transactional'='true','transactional_properties'='default'");
  }

  @Override
  @Test
  public void writeBetweenWorkerAndCleaner() throws Exception {
    writeBetweenWorkerAndCleanerForVariousTblProperties("'transactional'='true','transactional_properties'='default'");
  }

  @Override
  @Test
  public void testACIDwithSchemaEvolutionAndCompaction() throws Exception {
    testACIDwithSchemaEvolutionForVariousTblProperties("'transactional'='true','transactional_properties'='default'");
  }

  /**
   * In current implementation of ACID, altering the value of transactional_properties or trying to
   * set a value for previously unset value for an acid table will throw an exception.
   * @throws Exception
   */
  @Test
  public void testFailureOnAlteringTransactionalProperties() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("TBLPROPERTIES with 'transactional_properties' cannot be altered after the table is created");
    runStatementOnDriver("create table acidTblLegacy (a int, b int) clustered by (a) into " + BUCKET_COUNT + " buckets stored as orc TBLPROPERTIES ('transactional'='true')");
    runStatementOnDriver("alter table acidTblLegacy SET TBLPROPERTIES ('transactional_properties' = 'default')");
  }

  /**
   * Test the query correctness and directory layout for ACID table conversion with split-update
   * enabled.
   * 1. Insert a row to Non-ACID table
   * 2. Convert Non-ACID to ACID table with split-update enabled
   * 3. Insert a row to ACID table
   * 4. Perform Major compaction
   * 5. Clean
   * @throws Exception
   */
  @Test
  public void testNonAcidToAcidSplitUpdateConversion1() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert a row to Non-ACID table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 original bucket files in the location (000000_0 and 000001_0)
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    int [][] resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    int resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 2. Convert NONACIDORCTBL to ACID table
    runStatementOnDriver("alter table " + Table.NONACIDORCTBL
        + " SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default')");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // Everything should be same as before
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 3. Insert another row to newly-converted ACID table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(3,4)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 original bucket files (000000_0 and 000001_0), plus a new delta directory.
    // The delta directory should also have only 1 bucket file (bucket_00001)
    Assert.assertEquals(3, status.length);
    boolean sawNewDelta = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("delta_.*")) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Assert.assertEquals(1, buckets.length); // only one bucket file
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
      } else {
        Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
      }
    }
    Assert.assertTrue(sawNewDelta);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL + " order by a,b");
    resultData = new int[][] {{1, 2}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 4. Perform a major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new directory: base_xxxxxxx.
    // Original bucket files and delta directory should stay until Cleaner kicks in.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(4, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Assert.assertEquals(1, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
      }
    }
    Assert.assertTrue(sawNewBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 5. Let Cleaner delete obsolete files/dirs
    // Note, here we create a fake directory along with fake files as original directories/files
    String fakeFile0 = TEST_WAREHOUSE_DIR + "/" + (Table.NONACIDORCTBL).toString().toLowerCase() +
        "/subdir/000000_0";
    String fakeFile1 = TEST_WAREHOUSE_DIR + "/" + (Table.NONACIDORCTBL).toString().toLowerCase() +
        "/subdir/000000_1";
    fs.create(new Path(fakeFile0));
    fs.create(new Path(fakeFile1));
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // Before Cleaner, there should be 5 items:
    // 2 original files, 1 original directory, 1 base directory and 1 delta directory
    Assert.assertEquals(5, status.length);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_xxxxxxx.
    // Original bucket files and delta directory should have been cleaned up.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
    FileStatus[] buckets = fs.listStatus(status[0].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(1, buckets.length);
    Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));
  }

  /**
   * Test the query correctness and directory layout for ACID table conversion with split-update
   * enabled.
   * 1. Insert a row to Non-ACID table
   * 2. Convert Non-ACID to ACID table with split update enabled.
   * 3. Update the existing row in ACID table
   * 4. Perform Major compaction
   * 5. Clean
   * @throws Exception
   */
  @Test
  public void testNonAcidToAcidSplitUpdateConversion2() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert a row to Non-ACID table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 original bucket files in the location (000000_0 and 000001_0)
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    int [][] resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    int resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 2. Convert NONACIDORCTBL to ACID table
    runStatementOnDriver("alter table " + Table.NONACIDORCTBL
        + " SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default')");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // Everything should be same as before
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 3. Update the existing row in newly-converted ACID table
    runStatementOnDriver("update " + Table.NONACIDORCTBL + " set b=3 where a=1");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 original bucket files (000000_0 and 000001_0), plus one delta directory
    // and one delete_delta directory. When split-update is enabled, an update event is split into
    // a combination of delete and insert, that generates the delete_delta directory.
    // The delta directory should also have 2 bucket files (bucket_00000 and bucket_00001)
    // and so should the delete_delta directory.
    Assert.assertEquals(4, status.length);
    boolean sawNewDelta = false;
    boolean sawNewDeleteDelta = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("delta_.*")) {
        sawNewDelta = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
      } else if (status[i].getPath().getName().matches("delete_delta_.*")) {
        sawNewDeleteDelta = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_0000[01]"));
      } else {
        Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
      }
    }
    Assert.assertTrue(sawNewDelta);
    Assert.assertTrue(sawNewDeleteDelta);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 4. Perform a major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new directory: base_0000001.
    // Original bucket files and delta directory should stay until Cleaner kicks in.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(5, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
      }
    }
    Assert.assertTrue(sawNewBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 5. Let Cleaner delete obsolete files/dirs
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // Before Cleaner, there should be 5 items:
    // 2 original files, 1 delta directory, 1 delete_delta directory and 1 base directory
    Assert.assertEquals(5, status.length);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_0000001.
    // Original bucket files, delta directory and delete_delta should have been cleaned up.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertTrue(status[0].getPath().getName().matches("base_.*"));
    FileStatus[] buckets = fs.listStatus(status[0].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
    Assert.assertTrue(buckets[0].getPath().getName().matches("bucket_00001"));
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));
  }

  /**
   * Test the query correctness and directory layout for ACID table conversion with split-update
   * enabled.
   * 1. Insert a row to Non-ACID table
   * 2. Convert Non-ACID to ACID table with split-update enabled
   * 3. Perform Major compaction
   * 4. Insert a new row to ACID table
   * 5. Perform another Major compaction
   * 6. Clean
   * @throws Exception
   */
  @Test
  public void testNonAcidToAcidSplitUpdateConversion3() throws Exception {
    FileSystem fs = FileSystem.get(hiveConf);
    FileStatus[] status;

    // 1. Insert a row to Non-ACID table
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(1,2)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // There should be 2 original bucket files in the location (000000_0 and 000001_0)
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    List<String> rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    int [][] resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    int resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 2. Convert NONACIDORCTBL to ACID table with split_update enabled. (txn_props=default)
    runStatementOnDriver("alter table " + Table.NONACIDORCTBL
        + " SET TBLPROPERTIES ('transactional'='true', 'transactional_properties'='default')");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // Everything should be same as before
    Assert.assertEquals(BUCKET_COUNT, status.length);
    for (int i = 0; i < status.length; i++) {
      Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
    }
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 3. Perform a major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new directory: base_-9223372036854775808
    // Original bucket files should stay until Cleaner kicks in.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(3, status.length);
    boolean sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        Assert.assertEquals("base_-9223372036854775808", status[i].getPath().getName());
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
      }
    }
    Assert.assertTrue(sawNewBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 2}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 1;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 4. Update the existing row, and insert another row to newly-converted ACID table
    runStatementOnDriver("update " + Table.NONACIDORCTBL + " set b=3 where a=1");
    runStatementOnDriver("insert into " + Table.NONACIDORCTBL + "(a,b) values(3,4)");
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Arrays.sort(status);  // make sure delta_0000001_0000001_0000 appears before delta_0000002_0000002_0000
    // There should be 2 original bucket files (000000_0 and 000001_0), a base directory,
    // plus two new delta directories and one delete_delta directory that would be created due to
    // the update statement (remember split-update U=D+I)!
    Assert.assertEquals(6, status.length);
    int numDelta = 0;
    int numDeleteDelta = 0;
    sawNewBase = false;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("delta_.*")) {
        numDelta++;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Arrays.sort(buckets);
        if (numDelta == 1) {
          Assert.assertEquals("delta_0000001_0000001_0000", status[i].getPath().getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        } else if (numDelta == 2) {
          Assert.assertEquals("delta_0000002_0000002_0000", status[i].getPath().getName());
          Assert.assertEquals(1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        }
      } else if (status[i].getPath().getName().matches("delete_delta_.*")) {
        numDeleteDelta++;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Arrays.sort(buckets);
        if (numDeleteDelta == 1) {
          Assert.assertEquals("delete_delta_0000001_0000001_0000", status[i].getPath().getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        }
      } else if (status[i].getPath().getName().matches("base_.*")) {
        Assert.assertEquals("base_-9223372036854775808", status[i].getPath().getName());
        sawNewBase = true;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
        Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
      } else {
        Assert.assertTrue(status[i].getPath().getName().matches("00000[01]_0"));
      }
    }
    Assert.assertEquals(2, numDelta);
    Assert.assertEquals(1, numDeleteDelta);
    Assert.assertTrue(sawNewBase);

    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 5. Perform another major compaction
    runStatementOnDriver("alter table "+ Table.NONACIDORCTBL + " compact 'MAJOR'");
    runWorker(hiveConf);
    // There should be 1 new base directory: base_0000001
    // Original bucket files, delta directories, delete_delta directories and the
    // previous base directory should stay until Cleaner kicks in.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Arrays.sort(status);
    Assert.assertEquals(7, status.length);
    int numBase = 0;
    for (int i = 0; i < status.length; i++) {
      if (status[i].getPath().getName().matches("base_.*")) {
        numBase++;
        FileStatus[] buckets = fs.listStatus(status[i].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
        Arrays.sort(buckets);
        if (numBase == 1) {
          Assert.assertEquals("base_-9223372036854775808", status[i].getPath().getName());
          Assert.assertEquals(BUCKET_COUNT - 1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        } else if (numBase == 2) {
          // The new base dir now has two bucket files, since the delta dir has two bucket files
          Assert.assertEquals("base_0000002", status[i].getPath().getName());
          Assert.assertEquals(1, buckets.length);
          Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
        }
      }
    }
    Assert.assertEquals(2, numBase);
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));

    // 6. Let Cleaner delete obsolete files/dirs
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    // Before Cleaner, there should be 6 items:
    // 2 original files, 2 delta directories, 1 delete_delta directory and 2 base directories
    Assert.assertEquals(7, status.length);
    runCleaner(hiveConf);
    // There should be only 1 directory left: base_0000001.
    // Original bucket files, delta directories and previous base directory should have been cleaned up.
    status = fs.listStatus(new Path(TEST_WAREHOUSE_DIR + "/" +
        (Table.NONACIDORCTBL).toString().toLowerCase()), FileUtils.STAGING_DIR_PATH_FILTER);
    Assert.assertEquals(1, status.length);
    Assert.assertEquals("base_0000002", status[0].getPath().getName());
    FileStatus[] buckets = fs.listStatus(status[0].getPath(), FileUtils.STAGING_DIR_PATH_FILTER);
    Arrays.sort(buckets);
    Assert.assertEquals(1, buckets.length);
    Assert.assertEquals("bucket_00001", buckets[0].getPath().getName());
    rs = runStatementOnDriver("select a,b from " + Table.NONACIDORCTBL);
    resultData = new int[][] {{1, 3}, {3, 4}};
    Assert.assertEquals(stringifyValues(resultData), rs);
    rs = runStatementOnDriver("select count(*) from " + Table.NONACIDORCTBL);
    resultCount = 2;
    Assert.assertEquals(resultCount, Integer.parseInt(rs.get(0)));
  }
}
