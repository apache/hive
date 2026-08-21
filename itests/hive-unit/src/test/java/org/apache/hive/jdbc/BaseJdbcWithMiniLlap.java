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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.ddl.process.kill.KillQueriesOperation;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.FieldDesc;
import org.apache.hadoop.hive.llap.Row;
import org.apache.hadoop.hive.llap.Schema;
import org.apache.hadoop.io.NullWritable;

import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.mapred.InputFormat;

/**
 * Specialize this base class for different serde's/formats
 * {@link #beforeTest(boolean) beforeTest} should be called
 * by sub-classes in a {@link org.junit.BeforeClass} initializer
 */
public abstract class BaseJdbcWithMiniLlap {

  private static String dataFileDir;
  private static Path kvDataFilePath;
  private static Path dataTypesFilePath;
  private static Path over10KFilePath;

  protected static MiniHS2 miniHS2 = null;
  protected static HiveConf conf = null;
  protected static Connection hs2Conn = null;

  // This method should be called by sub-classes in a @BeforeClass initializer
  public static MiniHS2 beforeTest(HiveConf inputConf) throws Exception {
    conf = inputConf;
    miniHS2 = new MiniHS2(conf, MiniClusterType.LLAP);
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");
    dataTypesFilePath = new Path(dataFileDir, "datatypes.txt");
    over10KFilePath = new Path(dataFileDir, "over10k");
    Map<String, String> confOverlay = new HashMap<String, String>();
    miniHS2.start(confOverlay);
    return miniHS2;
  }

  static HiveConf defaultConf() throws Exception {
    String confDir = "../../data/conf/llap/";
    if (confDir != null && !confDir.isEmpty()) {
      HiveConf.setHiveSiteLocation(new URL("file://"+ new File(confDir).toURI().getPath() + "/hive-site.xml"));
      System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());
    }
    HiveConf defaultConf = new HiveConf();
    defaultConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    defaultConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    defaultConf.addResource(new URL("file://" + new File(confDir).toURI().getPath() + "/tez-site.xml"));
    return defaultConf;
  }

  @Before
  public void setUp() throws Exception {
    hs2Conn = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
  }

  public static Connection getConnection(String jdbcURL, String user, String pwd) throws SQLException {
    Connection conn = DriverManager.getConnection(jdbcURL, user, pwd);
    conn.createStatement().execute("set hive.support.concurrency = false");
    return conn;
  }

  @After
  public void tearDown() throws Exception {
    hs2Conn.close();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  protected void createTestTable(String tableName) throws Exception {
    createTestTable(hs2Conn, null, tableName, kvDataFilePath.toString());
  }

  public static void createTestTable(Connection connection, String database, String tableName, String srcFile) throws
    Exception {
    Statement stmt = connection.createStatement();

    if (database != null) {
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + database);
      stmt.execute("USE " + database);
    }

    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
        + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");

    // load data
    stmt.execute("load data local inpath '" + srcFile + "' into table " + tableName);

    ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
    assertTrue(res.next());
    assertEquals("val_238", res.getString(2));
    res.close();
    stmt.close();
  }

  protected void createDataTypesTable(String tableName) throws Exception {
    Statement stmt = hs2Conn.createStatement();

    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    // tables with various types
    stmt.execute("create table " + tableName
        + " (c1 int, c2 boolean, c3 double, c4 string,"
        + " c5 array<int>, c6 map<int,string>, c7 map<string,string>,"
        + " c8 struct<r:string,s:int,t:double>,"
        + " c9 tinyint, c10 smallint, c11 float, c12 bigint,"
        + " c13 array<array<string>>,"
        + " c14 map<int, map<int,int>>,"
        + " c15 struct<r:int,s:struct<a:int,b:string>>,"
        + " c16 array<struct<m:map<string,string>,n:int>>,"
        + " c17 timestamp, "
        + " c18 decimal(16,7), "
        + " c19 binary, "
        + " c20 date,"
        + " c21 varchar(20),"
        + " c22 char(15),"
        + " c23 binary"
        + ")");
    stmt.execute("load data local inpath '"
        + dataTypesFilePath.toString() + "' into table " + tableName);
    stmt.close();
  }

  protected void createOver10KTable(String tableName) throws Exception {
    try (Statement stmt = hs2Conn.createStatement()) {

      String createQuery =
          "create table " + tableName + " (t tinyint, si smallint, i int, b bigint, f float, d double, bo boolean, "
              + "s string, ts timestamp, `dec` decimal(4,2), bin binary) row format delimited fields terminated by '|'";

      // create table
      stmt.execute("DROP TABLE IF EXISTS " + tableName);
      stmt.execute(createQuery);
      // load data
      stmt.execute("load data local inpath '" + over10KFilePath.toString() + "' into table " + tableName);
    }
  }

  protected interface RowProcessor {
    void process(Row row);
  }

  protected static class RowCollector implements RowProcessor {
    ArrayList<String[]> rows = new ArrayList<String[]>();
    Schema schema = null;
    int numColumns = 0;

    public void process(Row row) {
      if (schema == null) {
        schema = row.getSchema();
        numColumns = schema.getColumns().size();
      }

      String[] arr = new String[numColumns];
      for (int idx = 0; idx < numColumns; ++idx) {
        Object val = row.getValue(idx);
        arr[idx] = (val == null ? null : val.toString());
      }
      rows.add(arr);
    }
  }

  // Save the actual values from each row as opposed to the String representation.
  protected static class RowCollector2 implements RowProcessor {
    ArrayList<Object[]> rows = new ArrayList<Object[]>();
    Schema schema = null;
    int numColumns = 0;

    public void process(Row row) {
      if (schema == null) {
        schema = row.getSchema();
        numColumns = schema.getColumns().size();
      }

      Object[] arr = new Object[numColumns];
      for (int idx = 0; idx < numColumns; ++idx) {
        arr[idx] = row.getValue(idx);
      }
      rows.add(arr);
    }
  }

  /**
   * Test CLI kill command of a query that is running.
   * We spawn 2 threads - one running the query and
   * the other attempting to cancel.
   * We're using a dummy udf to simulate a query,
   * that runs for a sufficiently long time.
   * @throws Exception
   */
  @Test
  public void testKillQuery() throws Exception {
    String tableName = "testtab1";
    createTestTable(tableName);
    Connection con = hs2Conn;
    Connection con2 = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");

    String udfName = TestJdbcWithMiniHS2.SleepMsUDF.class.getName();
    Statement stmt1 = con.createStatement();
    Statement stmt2 = con2.createStatement();
    stmt1.execute("create temporary function sleepMsUDF as '" + udfName + "'");
    stmt1.close();
    final Statement stmt = con.createStatement();

    ExceptionHolder tExecuteHolder = new ExceptionHolder();
    ExceptionHolder tKillHolder = new ExceptionHolder();

    // Thread executing the query
    Thread tExecute = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          System.out.println("Executing query: ");
          // The test table has 500 rows, so total query time should be ~ 500*500ms
          stmt.executeQuery("select sleepMsUDF(t1.under_col, 100), t1.under_col, t2.under_col " +
              "from " + tableName + " t1 join " + tableName + " t2 on t1.under_col = t2.under_col");
          fail("Expecting SQLException");
        } catch (SQLException e) {
          tExecuteHolder.throwable = e;
        }
      }
    });
    // Thread killing the query
    Thread tKill = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(2000);
          String queryId = ((HiveStatement) stmt).getQueryId();
          System.out.println("Killing query: " + queryId);

          stmt2.execute("kill query '" + queryId + "'");
          stmt2.close();
        } catch (Exception e) {
          tKillHolder.throwable = e;
        }
      }
    });

    tExecute.start();
    tKill.start();
    tExecute.join();
    tKill.join();
    stmt.close();
    con2.close();

    assertNotNull("tExecute", tExecuteHolder.throwable);
    assertEquals(HiveStatement.QUERY_CANCELLED_MESSAGE + " "+ KillQueriesOperation.KILL_QUERY_MESSAGE,
        tExecuteHolder.throwable.getMessage());
    assertNull("tCancel", tKillHolder.throwable);
  }

  private static class ExceptionHolder {
    Throwable throwable;
  }
}

