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

package org.apache.hive.service.server;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.SessionManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestGracefulStopHS2 {
  private static MiniHS2 miniHS2 = null;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    MiniHS2.cleanupLocalDir();
    try {
      HiveConf conf = new HiveConf();
      //TODO: HIVE-28296: TestGracefulStopHS2 to run on Tez
      conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "mr");
      conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
      conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED, false);
      conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_COL_AUTOGATHER, false);
      conf.setTimeVar(HiveConf.ConfVars.HIVE_SERVER2_GRACEFUL_STOP_TIMEOUT, 60, TimeUnit.SECONDS);
      MiniHS2.Builder builder = new MiniHS2.Builder().withConf(conf).cleanupLocalDirOnStartup(false);
      miniHS2 = builder.build();
      miniHS2.start(new HashMap<>());
    } catch (Exception e) {
      System.out.println("Unable to start MiniHS2: " + e);
      throw e;
    }
    try (Connection conn = getConnection()) {
      // no op here
    } catch (Exception e) {
      System.out.println("Unable to open default connections to MiniHS2: " + e);
      throw e;
    }
  }

  private static Connection getConnection() throws Exception {
    Connection conn = DriverManager.getConnection(miniHS2.getJdbcURL(),
        System.getProperty("user.name"), "");
    assertNotNull(conn);
    return conn;
  }

  @Test
  public void testGracefulStop() throws Exception {
    Connection conn = getConnection();
    Statement stmt = conn.createStatement();
    assertTrue(stmt.execute("select 1"));
    ExecutorService executors = Executors.newCachedThreadPool();
    Request req1 = new Request("select 'test', reflect(\"java.lang.Thread\", \"sleep\", bigint(20000))");
    Request req2 = new Request("select 'test', reflect(\"java.lang.Thread\", \"sleep\", bigint(600000))");
    Future future1 = executors.submit(req1), future2 = executors.submit(req2);
    Thread.sleep(1000);
    // Now decommission hs2
    executors.submit(() -> miniHS2.graceful_stop());
    executors.shutdown();
    Thread.sleep(1000);
    assertTrue(miniHS2.getOpenSessionsCount() == 3);
    try {
      // Fail to run new queries
      stmt.execute("set a=b");
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof HiveSQLException);
      assertTrue(e.getMessage().contains("Unable to run new queries as HiveServer2 is decommissioned or inactive"));
    }
    // Close existing connections with no errors
    stmt.close();
    conn.close();
    assertTrue(miniHS2.getOpenSessionsCount() == 2);
    try {
      // Fail to open new connections
      getConnection();
      fail();
    } catch (Exception e) {
      assertTrue(e.getMessage().contains(SessionManager.INACTIVE_ERROR_MESSAGE));
    }
    assertNull(req1.result);
    assertNull(req2.result);
    future1.get(); // finished
    assertTrue((Boolean)req1.result);
    future2.get();
    assertTrue(req2.result instanceof Exception); // timeout
  }

  private class Request implements Runnable {
    volatile Object result;
    final String query;
    Request(String query) {
      this.query = query;
    }
    @Override
    public void run() {
      try (Connection connection = getConnection();
           Statement stmt = connection.createStatement()) {
        if (stmt.execute(query)) {
          ResultSet rs = stmt.getResultSet();
          while (rs.next()) ;
        }
        result = true;
      } catch (Exception e) {
        result = e;
      }
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if ((miniHS2 != null) && (miniHS2.isStarted())) {
      miniHS2.stop();
    }
    if (miniHS2 != null) {
      miniHS2.cleanup();
    }
    MiniHS2.cleanupLocalDir();
  }
}
