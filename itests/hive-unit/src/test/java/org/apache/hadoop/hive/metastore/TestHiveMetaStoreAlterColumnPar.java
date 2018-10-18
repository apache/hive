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

package org.apache.hadoop.hive.metastore;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;

/**
 * Test that set/unset of metaconf:hive.metastore.disallow.incompatible.col.type.changes is local
 * to the session
 *
 * Two connections are created.  In one connection
 * metaconf:hive.metastore.disallow.incompatible.col.type.changes is set to false.  Then the
 * columm type can changed from int to smallint.  In the other connection
 * metaconf:hive.metastore.disallow.incompatible.col.type.changes is left with the default value
 * true.  In that connection changing column type from int to smallint will throw an error.
 *
 */
public class TestHiveMetaStoreAlterColumnPar {

  public static MiniHS2 miniHS2 = null;

  @BeforeClass
  public static void startServices() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS, 2);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS, 2);
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    miniHS2 = new MiniHS2.Builder().withMiniMR().withRemoteMetastore().withConf(hiveConf).build();

    miniHS2.start(new HashMap<String, String>());

    Connection hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(), "hive", "hive");
    Statement stmt = hs2Conn.createStatement();

    // create table
    stmt.execute("drop table if exists t1");
    stmt.execute("create table t1 (c1 int)");

    stmt.close();
    hs2Conn.close();
  }

  @AfterClass
  public static void stopServices() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testAlterColumn() throws Exception {
    assertTrue("Test setup failed. MiniHS2 is not initialized",
        miniHS2 != null && miniHS2.isStarted());

    try (
        Connection hs2Conn1 = DriverManager
            .getConnection(TestHiveMetaStoreAlterColumnPar.miniHS2.getJdbcURL(), "hive", "hive");
        Statement stmt1 = hs2Conn1.createStatement();
        Connection hs2Conn2 = DriverManager
            .getConnection(TestHiveMetaStoreAlterColumnPar.miniHS2.getJdbcURL(), "hive", "hive");
        Statement stmt2 = hs2Conn2.createStatement();
    ) {
      // Set parameter to be false in connection 1.  int to smallint allowed
      stmt1.execute("set metaconf:hive.metastore.disallow.incompatible.col.type.changes=false");
      stmt1.execute("alter table t1 change column c1 c1 smallint");

      // Change the type back to int so that the same alter can be attempted from connection 2.
      stmt1.execute("alter table t1 change column c1 c1 int");

      // parameter value not changed to false in connection 2.  int to smallint throws exception
      try {
        stmt2.execute("alter table t1 change column c1 c1 smallint");
        fail("Exception not thrown");
      } catch (Exception e1) {
        assertTrue("Unexpected exception: " + e1.getMessage(), e1.getMessage().contains(
            "Unable to alter table. The following columns have types incompatible with the existing columns in their respective positions"));
      }

      // parameter value is still false in 1st connection.  The alter still goes through.
      stmt1.execute("alter table t1 change column c1 c1 smallint");
    } catch (Exception e2) {
      fail("Unexpected Exception: " + e2.getMessage());
    }
  }
}
