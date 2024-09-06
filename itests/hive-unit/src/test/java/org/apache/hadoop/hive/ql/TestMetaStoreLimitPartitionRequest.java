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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.HiveSQLException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * This unit test is for testing HIVE-13884 with more complex queries and
 * hive.metastore.limit.partition.request enabled.
 * It covers cases when the query predicates can be pushed down and the
 * number of partitions can be retrieved via directSQL.
 * It also covers cases when the number of partitions cannot be retrieved
 * via directSQL, so it falls back to ORM.
 */
public class TestMetaStoreLimitPartitionRequest {

  private static final String DB_NAME = "max_partition_test_db";
  private static final String TABLE_NAME = "max_partition_test_table";
  private static int PARTITION_REQUEST_LIMIT = 4;
  private static MiniHS2 miniHS2 = null;
  private static HiveConf conf;
  private Connection hs2Conn = null;
  private Statement stmt;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    conf = new HiveConfForTest(TestMetaStoreLimitPartitionRequest.class);
    DriverManager.setLoginTimeout(0);

    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setIntVar(HiveConf.ConfVars.METASTORE_LIMIT_PARTITION_REQUEST, PARTITION_REQUEST_LIMIT);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_INTEGER_JDO_PUSHDOWN, true);
    conf.setBoolVar(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL, true);
    conf.setBoolVar(HiveConf.ConfVars.DYNAMIC_PARTITIONING, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CBO_ENABLED, false);
    // Disable loading dynamic partitions from partition names in this test
    // because get_partitions_by_names will also hit partition limit since HIVE-28062.
    conf.setBoolVar(HiveConf.ConfVars.HIVE_LOAD_DYNAMIC_PARTITIONS_SCAN_SPECIFIC_PARTITIONS, false);

    miniHS2 = new MiniHS2.Builder().withConf(conf).build();
    Map<String, String> overlayProps = new HashMap<String, String>();
    miniHS2.start(overlayProps);
    createDb();
  }

  private static void createDb() throws Exception {
    Connection conn =
        DriverManager.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    Statement stmt2 = conn.createStatement();
    stmt2.execute("DROP DATABASE IF EXISTS " + DB_NAME + " CASCADE");
    stmt2.execute("CREATE DATABASE " + DB_NAME);
    stmt2.close();
    conn.close();
  }

  @Before
  public void setUp() throws Exception {
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(DB_NAME),
        System.getProperty("user.name"), "bar");
    stmt = hs2Conn.createStatement();
    stmt.execute("USE " + DB_NAME);
    createTable();
  }

  private void createTable() throws Exception {
    String tmpTableName = TABLE_NAME + "_tmp";
    stmt.execute("CREATE TABLE " + tmpTableName
        + " (id string, value string, num string, ds date) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' STORED AS TEXTFILE");
    stmt.execute("INSERT OVERWRITE TABLE " + tmpTableName
        + " VALUES ('1', 'value1', '25', '2008-04-09'), ('2', 'value2', '30', '2008-04-09'), "
        + "('3', 'value3', '35', '2008-04-09'), ('4', 'value4', '40', '2008-04-09'), "
        + "('5', 'value5', '25', '2008-05-09'), ('6', 'value6', '30', '2008-05-09'), "
        + "('7', 'value7', '35', '2008-05-09'), ('8', 'value8', '40', '2008-05-09'), "
        + "('9', 'value9', '25', '2009-04-09'), ('10', 'value10', '30', '2009-04-09'), "
        + "('11', 'value11', '35', '2009-04-09'), ('12', 'value12', '40', '2009-04-09')");

    stmt.execute("CREATE TABLE " + TABLE_NAME + " (id string, value string) PARTITIONED BY (num string, ds date)");
    stmt.execute("INSERT OVERWRITE TABLE " + TABLE_NAME + " PARTITION (num, ds) SELECT id, value, num, ds FROM " + tmpTableName);
  }

  @After
  public void tearDown() throws Exception {
    String tmpTableName = TABLE_NAME + "_tmp";
    stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);
    stmt.execute("DROP TABLE IF EXISTS " + tmpTableName);
    stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME + "_num_tmp");

    if (hs2Conn != null) {
      hs2Conn.close();
    }
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  /* Tests with queries which can be pushed down and executed with directSQL */

  @Test
  public void testSimpleQueryWithDirectSql() throws Exception {
    String queryString = "select value from %s where num='25' and ds='2008-04-09'";
    executeQuery(queryString, "value1");
  }

  @Test
  public void testMoreComplexQueryWithDirectSql() throws Exception {
    String queryString = "select value from %s where (ds between '2009-01-01' and '2009-12-31' and num='25') or (ds between '2008-01-01' and '2008-12-31' and num='30')";
    executeQuery(queryString, "value2", "value6", "value9");
  }

  /*
   * Tests with queries which can be pushed down and executed with directSQL, but the number of
   * partitions which should be fetched is bigger than the maximum set by the
   * hive.metastore.limit.partition.request parameter.
   */

  @Test
  public void testSimpleQueryWithDirectSqlTooManyPartitions() throws Exception {
    String queryString = "select value from %s where ds>'2008-04-20'";
    executeQueryExceedPartitionLimit(queryString, 5);
  }

  @Test
  public void testMoreComplexQueryWithDirectSqlTooManyPartitions() throws Exception {
    String queryString = "select value from %s where num='25' or (num='30' and ds between '2008-01-01' and '2008-12-31')";
    executeQueryExceedPartitionLimit(queryString, 5);
  }

  /*
   * Tests with queries which cannot be executed with directSQL, because of type mismatch. The type
   * of the num column is string, but the parameters used in the where clause are numbers. After
   * falling back to ORM, the number of partitions can be fetched by the
   * ObjectStore.getNumPartitionsViaOrmFilter method.
   */

  @Test
  public void testQueryWithFallbackToORM1() throws Exception {
    String queryString = "select value from %s where num!=25 and num!=35 and num!=40";
    executeQuery(queryString, "value2", "value6", "value10");
  }

  @Test
  public void testQueryWithFallbackToORMTooManyPartitions1() throws Exception {
    String queryString = "select value from %s where num=30 or num=25";
    executeQueryExceedPartitionLimit(queryString, 6);
  }

  /*
   * Tests with queries which cannot be executed with directSQL, because of type mismatch. The type
   * of the num column is string, but the parameters used in the where clause are numbers. After
   * falling back to ORM the number of partitions cannot be fetched by the
   * ObjectStore.getNumPartitionsViaOrmFilter method. They are fetched by the
   * ObjectStore.getPartitionNamesPrunedByExprNoTxn method.
   */

  @Test
  public void testQueryWithFallbackToORM2() throws Exception {
    String queryString = "select value from %s where num!=25 and ds='2008-04-09'";
    executeQuery(queryString, "value2", "value3", "value4");
  }

  @Test
  public void testQueryWithFallbackToORM3() throws Exception {
    String queryString = "select value from %s where num between 26 and 31";
    executeQuery(queryString, "value2", "value6", "value10");
  }

  @Test
  public void testQueryWithFallbackToORMTooManyPartitions2() throws Exception {
    String queryString = "select value from %s where num!=25 and (ds='2008-04-09' or ds='2008-05-09')";
    executeQueryExceedPartitionLimit(queryString, 6);
  }

  @Test
  public void testQueryWithFallbackToORMTooManyPartitions3() throws Exception {
    String queryString = "select value from %s where num>=30";
    executeQueryExceedPartitionLimit(queryString, 9);
  }

  @Test
  public void testQueryWithFallbackToORMTooManyPartitions4() throws Exception {
    String queryString = "select value from %s where num between 20 and 50";
    executeQueryExceedPartitionLimit(queryString, 12);
  }

  /*
   * Tests with queries which cannot be executed with directSQL, because the contain like or in.
   * After falling back to ORM the number of partitions cannot be fetched by the
   * ObjectStore.getNumPartitionsViaOrmFilter method. They are fetched by the
   * ObjectStore.getPartitionNamesPrunedByExprNoTxn method.
   */

  @Test
  public void testQueryWithInWithFallbackToORM() throws Exception {
    setupNumTmpTable();
    String queryString = "select value from %s a where ds='2008-04-09' and a.num in (select value from " + TABLE_NAME + "_num_tmp)";
    executeQuery(queryString, "value1", "value2");
  }

  @Test
  public void testQueryWithInWithFallbackToORMTooManyPartitions() throws Exception {
    setupNumTmpTable();
    String queryString = "select value from %s a where a.num in (select value from " + TABLE_NAME + "_num_tmp)";
    executeQueryExceedPartitionLimit(queryString, 5);
  }

  @Test
  public void testQueryWithInWithFallbackToORMTooManyPartitions2() throws Exception {
    setupNumTmpTable();
    String queryString = "select value from %s a where a.num in (select value from " + TABLE_NAME + "_num_tmp where value='25')";
    executeQueryExceedPartitionLimit(queryString, 5);
  }

  @Test
  public void testQueryWithLikeWithFallbackToORMTooManyPartitions() throws Exception {
    String queryString = "select value from %s where num like '3%%'";
    executeQueryExceedPartitionLimit(queryString, 5);
  }

  private void setupNumTmpTable() throws SQLException {
    stmt.execute("CREATE TABLE " + TABLE_NAME + "_num_tmp (value string)");
    stmt.execute("INSERT INTO " + TABLE_NAME + "_num_tmp VALUES ('25')");
    stmt.execute("INSERT INTO " + TABLE_NAME + "_num_tmp VALUES ('30')");
  }

  private void executeQuery(String query, String... expectedValues) throws SQLException {
    String queryStr = String.format(query, TABLE_NAME);
    ResultSet result = stmt.executeQuery(queryStr);
    assertTrue(result != null);
    Set<String> expectedValueSet = new HashSet<>(Arrays.asList(expectedValues));
    Set<String> resultValues = getResultValues(result);
    String errorMsg = getWrongResultErrorMsg(queryStr, expectedValueSet.toString(), resultValues.toString());
    assertTrue(errorMsg, resultValues.equals(expectedValueSet));
  }

  private Set<String> getResultValues(ResultSet result) throws SQLException {
    Set<String> resultValues = new HashSet<>();
    while(result.next()) {
      resultValues.add(result.getString(1));
    }
    return resultValues;
  }

  private void executeQueryExceedPartitionLimit(String query, int expectedPartitionNumber) throws Exception {
    try {
      String queryStr = String.format(query, TABLE_NAME);
      stmt.executeQuery(queryStr);
      fail("The query should have failed, because the number of requested partitions are bigger than "
              + PARTITION_REQUEST_LIMIT);
    } catch (HiveSQLException e) {
      String exceedLimitMsg = String.format(HMSHandler.PARTITION_NUMBER_EXCEED_LIMIT_MSG, expectedPartitionNumber,
          TABLE_NAME, PARTITION_REQUEST_LIMIT, MetastoreConf.ConfVars.LIMIT_PARTITION_REQUEST.toString());
      assertTrue(getWrongExceptionMessage(exceedLimitMsg, e.getMessage()),
          e.getMessage().contains(exceedLimitMsg.toString()));
    }
  }

  private String getWrongResultErrorMsg(String query, String expectedValues, String resultValues) {
    StringBuilder errorMsg = new StringBuilder();
    errorMsg.append("The query '");
    errorMsg.append(query);
    errorMsg.append("' returned wrong values. It returned the values ");
    errorMsg.append(resultValues);
    errorMsg.append(" instead of the expected ");
    errorMsg.append(expectedValues);
    return errorMsg.toString();
  }

  private String getWrongExceptionMessage(String exceedLimitMsg, String exceptionMessage) {
    StringBuilder errorMsg = new StringBuilder();
    errorMsg.append("The message of the exception doesn't contain the expected '");
    errorMsg.append(exceedLimitMsg.toString());
    errorMsg.append("'. It is: ");
    errorMsg.append(exceptionMessage);
    return errorMsg.toString();
  }

}
