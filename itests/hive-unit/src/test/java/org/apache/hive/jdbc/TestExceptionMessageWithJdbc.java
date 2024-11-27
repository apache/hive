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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.hooks.HiveProtoLoggingHook.ExecutionMode;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.HiveSQLException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestExceptionMessageWithJdbc {
  private static MiniHS2 miniHS2;
  private static Connection conDefault;
  private static Connection conTestDb;
  private static final String TEST_DB_NAME = "test_logging_level";
  private static final String USERNAME = System.getProperty("user.name");
  private static final String PASSWORD = "bar";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    MiniHS2.cleanupLocalDir();
    HiveConf conf = getNewHiveConf();
    try {
      startMiniHS2(conf);
    } catch (Exception e) {
      System.out.println("Unable to start MiniHS2: " + e);
      throw e;
    }

    try {
      openDefaultConnections();
    } catch (Exception e) {
      System.out.println("Unable to open default connections to MiniHS2: " + e);
      throw e;
    }
    Statement stmt = conDefault.createStatement();
    stmt.execute("drop database if exists " + TEST_DB_NAME + " cascade");
    stmt.execute("create database " + TEST_DB_NAME);
    stmt.close();

    try {
      openTestConnections();
    } catch (Exception e) {
      System.out.println("Unable to open default connections to MiniHS2: " + e);
      throw e;
    }
  }

  private static Connection getConnection() throws Exception {
    return getConnection(miniHS2.getJdbcURL(), USERNAME, PASSWORD);
  }

  private static Connection getConnection(String dbName) throws Exception {
    return getConnection(miniHS2.getJdbcURL(dbName), USERNAME, PASSWORD);
  }

  private static Connection getConnection(String jdbcURL, String user, String pwd)
      throws SQLException {
    Connection conn = DriverManager.getConnection(jdbcURL, user, pwd);
    assertNotNull(conn);
    return conn;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    Statement stmt = conDefault.createStatement();
    stmt.execute("set hive.support.concurrency = false");
    stmt.execute("drop database if exists " + TEST_DB_NAME + " cascade");
    stmt.close();
    if (conTestDb != null) {
      conTestDb.close();
    }
    if (conDefault != null) {
      conDefault.close();
    }
    stopMiniHS2();
    cleanupMiniHS2();
  }

  private static void startMiniHS2(HiveConf conf) throws Exception {
    MiniHS2.Builder builder = new MiniHS2.Builder().withConf(conf).cleanupLocalDirOnStartup(false);
    miniHS2 = builder.build();
    
    Map<String, String> confOverlay = new HashMap<>();
    miniHS2.start(confOverlay);
  }

  private static void stopMiniHS2() {
    if ((miniHS2 != null) && (miniHS2.isStarted())) {
      miniHS2.stop();
    }
  }

  private static void cleanupMiniHS2() throws IOException {
    if (miniHS2 != null) {
      miniHS2.cleanup();
    }
    MiniHS2.cleanupLocalDir();
  }

  private static void openDefaultConnections() throws Exception {
    conDefault = getConnection();
  }

  private static void openTestConnections() throws Exception {
    conTestDb = getConnection(TEST_DB_NAME);
  }

  private static HiveConf getNewHiveConf() {
    HiveConf conf = new HiveConfForTest(TestExceptionMessageWithJdbc.class);
    conf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, ExecutionMode.TEZ.name());
    return conf;
  }

  @Test
  public void testExceptionMessageContainsQueryId() throws Exception {
    try (Statement stmt = conTestDb.createStatement()) {
      try {
        stmt.execute("select * from foo");
        fail("Expected to be unreachable");
      } catch (Exception e) {
        assertTrue(e.getMessage().toLowerCase().contains(HiveSQLException.QUERY_ID.toLowerCase()));
      }
    }
  }
}
