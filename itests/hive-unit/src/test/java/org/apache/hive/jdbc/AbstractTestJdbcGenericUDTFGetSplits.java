/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.jdbc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapBaseInputFormat;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * AbstractTestJdbcGenericUDTFGetSplits.
 */
public abstract class AbstractTestJdbcGenericUDTFGetSplits {
  protected static MiniHS2 miniHS2 = null;
  protected static String dataFileDir;
  protected static String tableName = "testtab1";
  protected static String partitionedTableName = "partitionedtesttab1";
  protected static HiveConf conf = null;
  static Path kvDataFilePath;
  protected Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());

    String confDir = "../../data/conf/llap/";
    HiveConf.setHiveSiteLocation(new URL("file://" + new File(confDir).toURI().getPath() + "/hive-site.xml"));
    System.out.println("Setting hive-site: " + HiveConf.getHiveSiteLocation());

    conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES, "default");
    conf.setTimeVar(HiveConf.ConfVars.HIVE_TRIGGER_VALIDATION_INTERVAL, 100, TimeUnit.MILLISECONDS);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS, true);
    conf.setBoolVar(HiveConf.ConfVars.TEZ_EXEC_SUMMARY, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STRICT_CHECKS_CARTESIAN, false);
    conf.setVar(HiveConf.ConfVars.LLAP_IO_MEMORY_MODE, "none");
    conf.setVar(HiveConf.ConfVars.LLAP_EXTERNAL_SPLITS_TEMP_TABLE_STORAGE_FORMAT, "text");

    conf.addResource(new URL("file://" + new File(confDir).toURI().getPath() + "/tez-site.xml"));

    miniHS2 = new MiniHS2(conf, MiniHS2.MiniClusterType.LLAP);
    dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    kvDataFilePath = new Path(dataFileDir, "kv1.txt");

    Map<String, String> confOverlay = new HashMap<>();
    miniHS2.start(confOverlay);
    miniHS2.getDFS().getFileSystem().mkdirs(new Path("/apps_staging_dir/anonymous"));
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    hs2Conn = BaseJdbcWithMiniLlap.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
  }

  @After
  public void tearDown() throws Exception {
    LlapBaseInputFormat.closeAll();
    hs2Conn.close();
  }

  protected void runQuery(final String query, final List<String> setCmds, final int numRows) throws Exception {

    Connection con = hs2Conn;
    BaseJdbcWithMiniLlap.createTestTable(con, null, tableName, kvDataFilePath.toString());

    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setErr(new PrintStream(baos)); // capture stderr
    final Statement selStmt = con.createStatement();
    Throwable throwable = null;
    int rowCount = 0;
    try {
      try {
        if (setCmds != null) {
          for (String setCmd : setCmds) {
            selStmt.execute(setCmd);
          }
        }
        ResultSet resultSet = selStmt.executeQuery(query);
        while (resultSet.next()) {
          rowCount++;
        }
      } catch (SQLException e) {
        throwable = e;
      }
      selStmt.close();
      assertNull(throwable);
      System.out.println("Expected " + numRows + " rows for query '" + query + "'. Got: " + rowCount);
      assertEquals("Expected rows: " + numRows + " got: " + rowCount, numRows, rowCount);
    } finally {
      baos.close();
    }

  }

  protected List<String> getConfigs(String... more) {
    List<String> setCmds = new ArrayList<>();
    setCmds.add("set mapred.min.split.size=10");
    setCmds.add("set mapred.max.split.size=10");
    setCmds.add("set tez.grouping.min-size=10");
    setCmds.add("set tez.grouping.max-size=10");
    // to get at least 10 splits
    setCmds.add("set tez.grouping.split-waves=10");
    if (more != null) {
      setCmds.addAll(Arrays.asList(more));
    }
    return setCmds;
  }

  protected void testGenericUDTFOrderBySplitCount1(String udtfName, int[] expectedCounts) throws Exception {
    String query = "select " + udtfName + "(" + "'select value from " + tableName + "', 10)";
    runQuery(query, getConfigs(), expectedCounts[0]);

    // Check number of splits is respected
    query = "select get_splits(" + "'select value from " + tableName + "', 3)";
    runQuery(query, getConfigs(), 3);

    query = "select " + udtfName + "(" + "'select value from " + tableName + " order by under_col', 5)";
    runQuery(query, getConfigs(), expectedCounts[1]);

    query = "select " + udtfName + "(" + "'select value from " + tableName + " order by under_col limit 0', 5)";
    runQuery(query, getConfigs(), expectedCounts[2]);

    query = "select " + udtfName + "(" + "'select value from " + tableName + " limit 2', 5)";
    runQuery(query, getConfigs(), expectedCounts[3]);

    query = "select " + udtfName + "(" + "'select value from " + tableName + " group by value limit 2', 5)";
    runQuery(query, getConfigs(), expectedCounts[4]);

    query = "select " + udtfName + "(" + "'select value from " + tableName + " where value is not null limit 2', 5)";
    runQuery(query, getConfigs(), expectedCounts[5]);

    query = "select " + udtfName + "(" + "'select `value` from (select value from " + tableName +
        " where value is not null order by value) as t', 5)";
    runQuery(query, getConfigs(), expectedCounts[6]);
  }

  protected void testGenericUDTFOrderBySplitCount1OnPartitionedTable(String udtfName, int[] expectedCounts)
          throws Exception {
    createPartitionedTestTable(null, partitionedTableName);

    String query = "select " + udtfName + "(" + "'select id from " + partitionedTableName + "', 5)";
    runQuery(query, getConfigs(), expectedCounts[0]);

    query = "select " + udtfName + "(" + "'select id from " + partitionedTableName + " order by id', 5)";
    runQuery(query, getConfigs(), expectedCounts[1]);

    query = "select " + udtfName + "(" + "'select id from " + partitionedTableName + " limit 2', 5)";
    runQuery(query, getConfigs(), expectedCounts[2]);

    query = "select " + udtfName + "(" + "'select id from " + partitionedTableName + " where id != 0 limit 2', 5)";
    runQuery(query, getConfigs(), expectedCounts[3]);

    query = "select " + udtfName + "(" + "'select id from " + partitionedTableName + " group by id limit 2', 5)";
    runQuery(query, getConfigs(), expectedCounts[4]);

  }

  private void createPartitionedTestTable(String database, String tableName) throws Exception {
    Statement stmt = hs2Conn.createStatement();

    if (database != null) {
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + database);
      stmt.execute("USE " + database);
    }

    // create table
    stmt.execute("DROP TABLE IF EXISTS " + tableName);
    stmt.execute("CREATE TABLE " + tableName
            + " (id INT) partitioned by (p1 int)");

    // load data
    for (int i=1; i<=5; i++) {
      String values = "";
      for (int j=1; j<=10; j++) {
        if (j != 10) {
          values+= "(" + j +"),";
        } else {
          values+= "(" + j +")";
        }
      }
      stmt.execute("insert into " + tableName + " partition (p1=" + i +") " + " values " + values);
    }


    ResultSet res = stmt.executeQuery("SELECT count(*) FROM " + tableName);
    assertTrue(res.next());
    assertEquals(50, res.getInt(1));
    res.close();
    stmt.close();
  }
}
