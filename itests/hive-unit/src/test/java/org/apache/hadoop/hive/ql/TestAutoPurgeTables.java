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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAutoPurgeTables {

  private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  private static final String testDbName = "auto_purge_test_db";
  //private static final String testTableName = "auto_purge_test_table";
  private static final String INSERT_OVERWRITE_COMMAND_FORMAT =
      "insert overwrite table " + testDbName + ".%s select 1, \"test\"";
  private static final String TRUNCATE_TABLE_COMMAND_FORMAT =
      "truncate table " + testDbName + ".%s";
  private static final String partitionedColumnName = "partCol";
  private static final String partitionedColumnValue1 = "20090619";
  private static final String INSERT_OVERWRITE_COMMAND_PARTITIONED_FORMAT =
      "insert overwrite table " + testDbName + ".%s PARTITION ("
          + partitionedColumnName + "=" + partitionedColumnValue1 + ")" + " select 1, \"test\"";
  private static final String partitionedColumnValue2 = "20100720";
  private static HiveConf conf;
  private static Connection con;
  private static MiniHS2 miniHS2;
  static final private Logger LOG = LoggerFactory.getLogger("TestAutoPurgeTables");

  @Rule
  public TestName name = new TestName();

  private static Connection getConnection(String url) throws SQLException {
    Connection con1;
    con1 = DriverManager.getConnection(url, "", "");
    Assert.assertNotNull("Connection is null", con1);
    Assert.assertFalse("Connection should not be closed", con1.isClosed());
    return con1;
  }

  private static void createTestTable(Statement stmt, String isAutopurge, boolean isExternal,
      boolean isPartitioned, String testTableName) throws SQLException {
    String createTablePrefix;
    if (isExternal) {
      createTablePrefix = "create external table ";
    } else {
      createTablePrefix = "create table ";
    }
    String qualifiedTableName = StatsUtils.getFullyQualifiedTableName(testDbName, testTableName);
    if (isPartitioned) {
      // create a partitioned table
      stmt.execute(
          createTablePrefix + qualifiedTableName + " (id int, value string) "
          + " partitioned by (" + partitionedColumnName + " STRING)");
      // load data
      stmt.execute("insert into " + qualifiedTableName + " PARTITION ("
          + partitionedColumnName + "=" + partitionedColumnValue1
          + ") values (1, \"dummy1\"), (2, \"dummy2\"), (3, \"dummy3\")");
      stmt.execute("insert into " + qualifiedTableName + " PARTITION ("
          + partitionedColumnName + "=" + partitionedColumnValue2
          + ") values (4, \"dummy4\"), (5, \"dummy5\"), (6, \"dummy6\")");
    } else {
      // create a table
      stmt.execute(createTablePrefix + qualifiedTableName + " (id int, value string)");
      // load data
      stmt.execute("insert into " + qualifiedTableName
          + " values (1, \"dummy1\"), (2, \"dummy2\"), (3, \"dummy3\")");
    }
    if (isAutopurge != null) {
      stmt.execute("alter table " + qualifiedTableName
          + " set tblproperties (\"auto.purge\"=\"" + isAutopurge + "\")");
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = new HiveConfForTest(TestAutoPurgeTables.class);
    // enable trash so it can be tested
    conf.setFloat("fs.trash.checkpoint.interval", 30);
    conf.setFloat("fs.trash.interval", 30);
    // Create test database and base tables once for all the test
    miniHS2 = new MiniHS2.Builder().withConf(conf).build();
    miniHS2.start(new HashMap<String, String>());
    Class.forName(driverName);
    con = getConnection(miniHS2.getBaseJdbcURL() + ";create=true");
    try (Statement stmt = con.createStatement()) {
      Assert.assertNotNull("Statement is null", stmt);
      stmt.execute("set hive.support.concurrency = false");
      stmt.execute("drop database if exists " + testDbName + " cascade");
      stmt.execute("create database " + testDbName);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() {
    Statement stmt = null;
    try {
      stmt = con.createStatement();
      // drop test db and its tables and views
      stmt.execute("set hive.support.concurrency = false");
      stmt.execute("drop database if exists " + testDbName + " cascade");
      FileSystem fs = FileSystem.get(conf);
      fs.deleteOnExit(ShimLoader.getHadoopShims().getCurrentTrashPath(conf, fs));
    } catch (SQLException | IOException e) {
      e.printStackTrace();
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException e) {
          //
        }
      }
      if (con != null) {
        try {
          con.close();
        } catch (SQLException e) {
          //
        }
      }
      if (miniHS2 != null) {
        miniHS2.cleanup();
        miniHS2.stop();
        miniHS2 = null;
      }
    }
  }

  @Before
  public void beforeTest() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Path trashDir = ShimLoader.getHadoopShims().getCurrentTrashPath(conf, fs);
    fs.delete(trashDir, true);
  }

  @After
  public void afterTest() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Path trashDir = ShimLoader.getHadoopShims().getCurrentTrashPath(conf, fs);
    fs.delete(trashDir, true);
  }

  /**
   * Tests if previous table data skips trash when insert overwrite table .. is run against a table
   * which has auto.purge property set
   *
   * @throws Exception
   */
  @Test
  public void testAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("true", false, false, false, name.getMethodName());
  }

  /**
   * Tests when auto.purge is set to a invalid string, trash should be used for insert overwrite
   * queries
   * 
   * @throws Exception
   */
  @Test
  public void testAutoPurgeInvalid() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("invalid", false, false, false, name.getMethodName());
  }

  /**
   * Test when auto.purge property is not set. Data should be moved to trash for insert overwrite
   * queries
   * 
   * @throws Exception
   */
  @Test
  public void testAutoPurgeUnset() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil(null, false, false, false, name.getMethodName());
  }

  /**
   * Tests if the auto.purge property works correctly for external tables. Old data should skip
   * trash when insert overwrite table .. is run when auto.purge is set to true
   *
   * @throws Exception
   */
  @Test
  public void testExternalTable() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("true", true, false, false, name.getMethodName());
  }

  /**
   * Tests auto.purge when managed table is partitioned. Old data should skip trash when insert
   * overwrite table .. is run and auto.purge property is set to true
   *
   * @throws Exception
   */
  @Test
  public void testPartitionedTable() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("true", false, true, false, name.getMethodName());
  }

  /**
   * Tests auto.purge for an external, partitioned table. Old partition data should skip trash when
   * auto.purge is set to true
   *
   * @throws Exception
   */
  @Test
  public void testExternalPartitionedTable() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("true", true, true, false, name.getMethodName());
  }

  /**
   * Tests when auto.purge is set to false, older data is moved to Trash when insert overwrite table
   * .. is run
   *
   * @throws Exception
   */
  @Test
  public void testNoAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("false", false, false, false, name.getMethodName());
  }

  /**
   * Tests when auto.purge is set to false on a external table, older data is moved to Trash when
   * insert overwrite table .. is run
   *
   * @throws Exception
   */
  @Test
  public void testExternalNoAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("false", true, false, false, name.getMethodName());
  }

  /**
   * Tests when auto.purge is set to false on a partitioned table, older data is moved to Trash when
   * insert overwrite table .. is run
   *
   * @throws Exception
   */
  @Test
  public void testPartitionedNoAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("false", false, true, false, name.getMethodName());
  }

  /**
   * Tests when auto.purge is set to false on a partitioned external table, older data is moved to
   * Trash when insert overwrite table .. is run
   *
   * @throws Exception
   */
  @Test
  public void testPartitionedExternalNoAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("false", true, true, false, name.getMethodName());
  }

  //truncate on external table is not allowed
  @Test(expected = SQLException.class)
  public void testTruncatePartitionedExternalNoAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil(String.valueOf(false), true, true, true, name.getMethodName());
  }

  //truncate on external table is not allowed
  @Test(expected = SQLException.class)
  public void testTruncateExternalNoAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil(String.valueOf(false), true, false, true, name.getMethodName());
  }

  @Test
  public void testTruncatePartitionedNoAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil(String.valueOf(false), false, true, true, name.getMethodName());
  }

  @Test
  public void testTruncateNoAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil(String.valueOf(false), false, false, true, name.getMethodName());
  }

  @Test
  public void testTruncateInvalidAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil("invalid", false, false, true, name.getMethodName());
  }

  @Test
  public void testTruncateUnsetAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil(null, false, false, true, name.getMethodName());
  }

  //truncate on external table is not allowed
  @Test(expected = SQLException.class)
  public void testTruncatePartitionedExternalAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil(String.valueOf(true), true, true, true, name.getMethodName());
  }

  //truncate on external table is not allowed
  @Test(expected = SQLException.class)
  public void testTruncateExternalAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil(String.valueOf(true), true, false, true, name.getMethodName());
  }

  @Test
  public void testTruncatePartitionedAutoPurge() throws Exception {
    LOG.info("Running " + name.getMethodName());
    testUtil(String.valueOf(true), false, true, true, name.getMethodName());
  }

  /**
   * Test util method to run the insert overwrite table or truncate table test on a table
   * 
   * @param autoPurgePropValue - string value of the auto.purge property for the test table. Ignored
   *          if null
   * @param isExternal - if set creates a external table for the test
   * @param isPartitioned - if set creates a partitioned table for the test
   * @param isTruncateTest - if set uses truncate table command for the test. Otherwise uses Insert
   *          overwrite table command for the test
   * @param testTableName - test table name
   * @throws Exception
   */
  private void testUtil(String autoPurgePropValue, boolean isExternal, boolean isPartitioned,
      boolean isTruncateTest, String testTableName) throws Exception {
    testUtil(autoPurgePropValue, isExternal, isPartitioned,
        !"true".equalsIgnoreCase(autoPurgePropValue), isTruncateTest, testTableName);
  }
  /**
   * Test util method to run the insert overwrite table or truncate table test on a table
   * 
   * @param isAutoPurge - If set, creates a table with auto.purge with the given value
   * @param isExternal - if set creates a external table for the test
   * @param isPartitioned - if set creates a partitioned table for the test
   * @param purgeExpected - if set the assert condition for the test is such that it expectes old
   *          table data to be moved to trash. If not creates a assert condition to make sure that
   *          data is not moved to trash
   * @param isTruncateTest - if set uses truncate table command for the test. Otherwise uses Insert
   *          overwrite table command for the test
   * @param testTableName - table name for the test table
   * @throws Exception
   */
  private void testUtil(String isAutoPurge, boolean isExternal, boolean isPartitioned,
      boolean purgeExpected, boolean isTruncateTest, String testTableName) throws Exception {
    try (Statement stmt = con.createStatement()) {
      // create a test table with auto.purge = true
      createTestTable(stmt, isAutoPurge, isExternal, isPartitioned, testTableName);
      int numFilesInTrashBefore = getTrashFileCount();
      String command = getCommand(isTruncateTest, isPartitioned, testTableName);
      stmt.execute(command);
      int numFilesInTrashAfter = getTrashFileCount();
      if (purgeExpected) {
        Assert.assertTrue(
            String.format(
                "Data should have been moved to trash. Number of files in trash: before : %d after %d",
                numFilesInTrashBefore, numFilesInTrashAfter),
            numFilesInTrashBefore < numFilesInTrashAfter);
      } else {
        Assert.assertEquals(
            String.format(
                "Data should not have been moved to trash. Number of files in trash: before : %d after %d",
                numFilesInTrashBefore, numFilesInTrashAfter),
            numFilesInTrashBefore, numFilesInTrashAfter);
      }
    }
  }

  private static String getCommand(boolean isTruncateTest, boolean isPartitioned, String testTableName) {
    if (isTruncateTest) {
      return String.format(TRUNCATE_TABLE_COMMAND_FORMAT, testTableName);
    } else if (isPartitioned) {
      return String.format(INSERT_OVERWRITE_COMMAND_PARTITIONED_FORMAT, testTableName);
    } else {
      return String.format(INSERT_OVERWRITE_COMMAND_FORMAT, testTableName);
    }
  }

  private int getTrashFileCount() throws Exception {
    FileSystem fs = FileSystem.get(conf);
    Path trashDir = ShimLoader.getHadoopShims().getCurrentTrashPath(conf, fs);
    return getFileCount(fs, trashDir);
  }

  private int getFileCount(FileSystem fs, Path path) throws Exception {
    try {
      int count = 0;
      if (!fs.exists(path)) {
        return count;
      }
      RemoteIterator<LocatedFileStatus> lfs = fs.listFiles(path, true);
      while (lfs.hasNext()) {
        LocatedFileStatus lf = lfs.next();
        LOG.info(lf.getPath().toString());
        if (lf.isFile()) {
          count++;
        }
      }
      return count;
    } catch (IOException e) {
      throw new Exception("Exception while list files on " + path, e);
    }
  }
}
