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

package org.apache.hive.jdbc;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.processors.ErasureProcessor;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.HadoopShims.HdfsErasureCodingShim;
import org.apache.hadoop.hive.shims.HadoopShims.MiniDFSShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.apache.hadoop.hive.ql.QTestMiniClusters.DEFAULT_TEST_EC_POLICY;
import static org.apache.hive.jdbc.TestJdbcWithMiniHS2.getDetailedTableDescription;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Run erasure Coding tests with jdbc.
 */
public class TestJdbcWithMiniHS2ErasureCoding {
  private static final String DB_NAME = "ecTestDb";
  private static MiniHS2 miniHS2 = null;
  private static HiveConf conf;
  private Connection hs2Conn = null;

    /**
     * Setup a mini HS2 with miniMR.
     */
  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    conf = new HiveConfForTest(TestJdbcWithMiniHS2ErasureCoding.class);
    //TODO: HIVE-28284: TestJdbcWithMiniHS2/TestJdbcWithMiniHS2ErasureCoding to run on Tez
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    DriverManager.setLoginTimeout(0);
    miniHS2 = new MiniHS2.Builder()
        .withConf(conf)
        .withMiniMR()
        .withDataNodes(5) // sufficient for RS-3-2-1024k
        .build();
    miniHS2.start(Collections.emptyMap());
    createDb();
    MiniDFSShim dfs = miniHS2.getDfs();
    addErasurePolicy(dfs, "hdfs:///", DEFAULT_TEST_EC_POLICY);
  }

  /**
   * Shutdown the mini HS2.
   */
  @AfterClass
  public static void afterTest() {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  /**
   * Setup a connection to the test database before each test.
   */
  @Before
  public void setUp() throws Exception {
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(DB_NAME),
        System.getProperty("user.name"), "bar");
  }

  /**
   * Close connection after each test.
   */
  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      hs2Conn.close();
    }
  }

  /**
   * Create a database.
   */
  private static void createDb() throws Exception {
    try (Connection conn = DriverManager.getConnection(miniHS2.getJdbcURL(),
            System.getProperty("user.name"), "bar");
         Statement stmt2 = conn.createStatement()) {
      stmt2.execute("DROP DATABASE IF EXISTS " + DB_NAME + " CASCADE");
      stmt2.execute("CREATE DATABASE " + DB_NAME);
    }
  }

  /**
   * Test EXPLAIN on fs with Erasure Coding.
   */
  @Test
  public void testExplainErasureCoding() throws Exception {
    try (Statement stmt = hs2Conn.createStatement()) {
      String tableName = "pTableEc";
      stmt.execute(
          " CREATE TABLE " + tableName + " (userid VARCHAR(64), link STRING, source STRING) "
              + "PARTITIONED BY (datestamp STRING, i int) "
              + "CLUSTERED BY (userid) INTO 4 BUCKETS STORED AS PARQUET");
      // insert data to create 2 partitions
      stmt.execute("INSERT INTO TABLE " + tableName
          + " PARTITION (datestamp = '2014-09-23', i = 1)(userid,link) VALUES ('jsmith', 'mail.com')");
      stmt.execute("INSERT INTO TABLE " + tableName
            + " PARTITION (datestamp = '2014-09-24', i = 2)(userid,link) VALUES ('mac', 'superchunk.com')");
      String explain = getExtendedExplain(stmt, "select userid from " + tableName);
      assertMatchAndCount(explain,  " numFilesErasureCoded 4", 2);
    }
  }

  /**
   * Test DESCRIBE on fs with Erasure Coding.
   */
  @Test
  public void testDescribeErasureCoding() throws Exception {
    try (Statement stmt = hs2Conn.createStatement()) {
      String table = "pageviews";
      stmt.execute(" CREATE TABLE " + table + " (userid VARCHAR(64), link STRING, source STRING) "
          + "PARTITIONED BY (datestamp STRING, i int) CLUSTERED BY (userid) INTO 4 BUCKETS STORED AS PARQUET");
      stmt.execute("INSERT INTO TABLE " + table + " PARTITION (datestamp = '2014-09-23', i = 1)"
          + "(userid,link) VALUES ('jsmith', 'mail.com')");
      stmt.execute("INSERT INTO TABLE " + table + " PARTITION (datestamp = '2014-09-24', i = 1)"
          + "(userid,link) VALUES ('dpatel', 'gmail.com')");
      String description = getDetailedTableDescription(stmt, table);
      assertMatchAndCount(description, "numFiles=8", 1);
      assertMatchAndCount(description, "numFilesErasureCoded=8", 1);
      assertMatchAndCount(description, "numPartitions=2", 1);
    }
  }

  /**
   * Test MR stats.
   */
  @Test
  public void testMapRedStats() throws Exception {
    // Do log4j magic to save log output
    StringWriter writer = new StringWriter();
    Appender appender = addAppender(writer, "testMapRedStats");
    try (Statement stmt = hs2Conn.createStatement()) {
      String table = "mapredstats";
      stmt.execute("set hive.execution.engine=mr");
      stmt.execute(" CREATE TABLE " + table + " (a int) STORED AS PARQUET");
      stmt.execute("INSERT INTO TABLE " + table + " VALUES (3)");
      try (ResultSet rs = stmt.executeQuery("select a from " + table + " order by a")) {
        while (rs.next()) {
          int val = rs.getInt(1);
          assertEquals(3, val);
        }
      }
    }
    String output = writer.toString();
    // check for standard stats
    assertTrue(output.contains("HDFS Read:"));
    assertTrue(output.contains("HDFS Write:"));

    // check for erasure coding stat
    HadoopShims.HdfsErasureCodingShim erasureShim = ErasureProcessor.getErasureShim(conf);
    if (erasureShim.isMapReduceStatAvailable()) {
      assertTrue(output.contains("HDFS EC Read:"));
    }
  }

  /**
   * Add an appender to log4j.
   * http://logging.apache.org/log4j/2.x/manual/customconfig.html#AddingToCurrent
   */
  private Appender addAppender(final Writer writer, final String writerName) {
    final LoggerContext context = LoggerContext.getContext(false);
    final Configuration config = context.getConfiguration();
    final PatternLayout layout = PatternLayout.createDefaultLayout(config);
    final Appender appender =
        WriterAppender.createAppender(layout, null, writer, writerName, false, true);
    appender.start();
    config.getRootLogger().addAppender(appender, null, null);
    return appender;
  }

  /**
   * Add a Erasure Coding Policy to a Path.
   */
  private static void addErasurePolicy(MiniDFSShim dfs, String pathString, String policyName) throws IOException {
    HadoopShims hadoopShims = ShimLoader.getHadoopShims();
    HdfsErasureCodingShim erasureCodingShim = hadoopShims.createHdfsErasureCodingShim(dfs.getFileSystem(), conf);
    erasureCodingShim.enableErasureCodingPolicy(policyName);
    Path fsRoot = new Path(pathString);
    erasureCodingShim.setErasureCodingPolicy(fsRoot, policyName);
    HadoopShims.HdfsFileErasureCodingPolicy erasureCodingPolicy =
        erasureCodingShim.getErasureCodingPolicy(fsRoot);
    assertEquals(policyName, erasureCodingPolicy.getName());
  }

  /**
   * Get Extended Explain output via jdbc.
   */
  private static String getExtendedExplain(Statement stmt, String query) throws SQLException {
    StringBuilder sb = new StringBuilder(2048);
    try (ResultSet rs = stmt.executeQuery("explain extended " + query)) {
      while (rs.next()) {
        sb.append(rs.getString(1)).append('\n');
      }
    }
    return sb.toString();
  }

  /**
   * Check that the expected string occurs correctly in the output string.
   * @param output string to probe
   * @param expectedString string to find in output
   * @param expectedCount the expected number of occurrences of the expected string
   */
  private void assertMatchAndCount(String output, String expectedString, int expectedCount) {
    assertTrue("Did not find expected '" + expectedString + "' in text " +
        output, output.contains(expectedString));
    assertEquals("wrong count of matches of '"  + expectedString + "' in text " +
        output, expectedCount, StringUtils.countMatches(output, expectedString));
  }
}
