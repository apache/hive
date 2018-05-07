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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHook;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMultiSessionsHS2WithLocalClusterSpark {
  public static final String TEST_TAG = "miniHS2.localClusterSpark.tag";
  public static final String TEST_TAG_VALUE = "miniHS2.localClusterSpark.value";
  private static final int PARALLEL_NUMBER = 3;

  public static class LocalClusterSparkSessionHook implements HiveSessionHook {
    @Override
    public void run(HiveSessionHookContext sessionHookContext) throws HiveSQLException {
      sessionHookContext.getSessionConf().set(TEST_TAG, TEST_TAG_VALUE);
    }
  }

  private static MiniHS2 miniHS2 = null;
  private static HiveConf conf;
  private static Path dataFilePath;
  private static String dbName = "sparkTestDb";
  private ThreadLocal<Connection> localConnection = new ThreadLocal<Connection>();
  private ThreadLocal<Statement> localStatement = new ThreadLocal<Statement>();
  private ExecutorService pool = null;


  private static HiveConf createHiveConf() {
    HiveConf conf = new HiveConf();
    conf.set("hive.exec.parallel", "true");
    conf.set("hive.execution.engine", "spark");
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    conf.set("spark.master", "local-cluster[2,2,1024]");
    conf.set("spark.deploy.defaultCores", "2");
    conf.set("hive.spark.client.connect.timeout", "30000ms");
    // FIXME: Hadoop3 made the incompatible change for dfs.client.datanode-restart.timeout
    // while spark2 is still using Hadoop2.
    // Spark requires Hive to support Hadoop3 first then Spark can start
    // working on Hadoop3 support. Remove this after Spark supports Hadoop3.
    conf.set("dfs.client.datanode-restart.timeout", "30");
    return conf;
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    conf = createHiveConf();
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/')
      .replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    DriverManager.setLoginTimeout(0);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    miniHS2 = new MiniHS2(conf, MiniClusterType.MR);
    Map<String, String> overlayProps = new HashMap<String, String>();
    overlayProps.put(ConfVars.HIVE_SERVER2_SESSION_HOOK.varname,
      LocalClusterSparkSessionHook.class.getName());
    miniHS2.start(overlayProps);
    createDb();
  }

  // setup DB
  private static void createDb() throws Exception {
    Connection conn = DriverManager.
      getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    Statement stmt2 = conn.createStatement();
    stmt2.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    stmt2.execute("CREATE DATABASE " + dbName);
    stmt2.close();
    conn.close();
  }

  @Before
  public void setUp() throws Exception {
    pool = Executors.newFixedThreadPool(PARALLEL_NUMBER,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Test-Thread-%d").build());
    createConnection();
  }

  @After
  public void tearDown() throws Exception {
    pool.shutdownNow();
    closeConnection();
  }

  private void createConnection() throws Exception {
    Connection connection = DriverManager.getConnection(miniHS2.getJdbcURL(dbName),
      System.getProperty("user.name"), "bar");
    Statement statement = connection.createStatement();
    localConnection.set(connection);
    localStatement.set(statement);
    statement.execute("USE " + dbName);
  }

  private void closeConnection() throws SQLException {
    if (localStatement.get() != null) {
      localStatement.get().close();
    }

    if (localConnection.get() != null) {
      localConnection.get().close();
    }
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  /**
   * Run nonSpark query
   *
   * @throws Exception
   */
  @Test
  public void testNonSparkQuery() throws Exception {
    String tableName = "kvTable1";
    setupTable(tableName);
    Callable<Void> runNonSparkQuery = getNonSparkQueryCallable(tableName);
    runInParallel(runNonSparkQuery);
    dropTable(tableName);
  }

  /**
   * Run spark query
   *
   * @throws Exception
   */
  @Test
  public void testSparkQuery() throws Exception {
    String tableName = "kvTable2";
    setupTable(tableName);
    Callable<Void> runSparkQuery = getSparkQueryCallable(tableName);
    runInParallel(runSparkQuery);
    dropTable(tableName);
  }

  private void runInParallel(Callable<Void> runNonSparkQuery) throws InterruptedException, ExecutionException {
    List<Future> futureList = new LinkedList<Future>();
    for (int i = 0; i < PARALLEL_NUMBER; i++) {
      Future future = pool.submit(runNonSparkQuery);
      futureList.add(future);
    }

    for (Future future : futureList) {
      future.get();
    }
  }

  private Callable<Void> getNonSparkQueryCallable(final String tableName) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String resultVal = "val_238";
        String queryStr = "SELECT * FROM " + tableName;
        testKvQuery(queryStr, resultVal);
        return null;
      }
    };
  }

  private Callable<Void> getSparkQueryCallable(final String tableName) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        String resultVal = "val_238";
        String queryStr = "SELECT * FROM " + tableName +
          " where value = '" + resultVal + "'";
        testKvQuery(queryStr, resultVal);
        return null;
      }
    };
  }

  private void testKvQuery(String queryStr, String resultVal)
    throws Exception {
    createConnection();
    verifyResult(queryStr, resultVal, 2);
    closeConnection();
  }

  // create table and load kv1.txt
  private void setupTable(String tableName) throws SQLException {
    Statement statement = localStatement.get();
    // create table
    statement.execute("CREATE TABLE " + tableName
      + " (under_col INT COMMENT 'the under column', value STRING)"
      + " COMMENT ' test table'");

    // load data
    statement.execute("LOAD DATA LOCAL INPATH '"
      + dataFilePath.toString() + "' INTO TABLE " + tableName);
  }

  private void dropTable(String tableName) throws SQLException {
    localStatement.get().execute("DROP TABLE " + tableName);
  }

  // run given query and validate expected result
  private void verifyResult(String queryStr, String expString, int colPos)
    throws SQLException {
    ResultSet res = localStatement.get().executeQuery(queryStr);
    assertTrue(res.next());
    assertEquals(expString, res.getString(colPos));
    res.close();
  }
}
