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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class TestMaterializedViewRebuildWithMiniHS2 {

  private static MiniHS2 miniHS2 = null;

  private static final String TABLE1 = "t1";
  private static final String MV1 = "mat1";

  @BeforeClass
  public static void setup() throws Exception {
    HiveConfForTest hiveConf = new HiveConfForTest(TestMaterializedViewRebuildWithMiniHS2.class);

    String testWarehouseDir = hiveConf.getTestDataDir() + "/warehouse";
    File f = new File(testWarehouseDir);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if (!(new File(testWarehouseDir).mkdirs())) {
      throw new RuntimeException("Could not create " + testWarehouseDir);
    }
    MetastoreConf.setVar(hiveConf, MetastoreConf.ConfVars.WAREHOUSE, testWarehouseDir);

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");


    miniHS2 = new MiniHS2.Builder()
            .withConf(hiveConf)
            .withRemoteMetastore(true)
            .build();

    miniHS2.start(new HashMap<>());
    hiveConf.set("iceberg.engine.hive.lock-enabled", "true");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testConcurrentMVRebuildWhenRollback() throws Exception {
    String connectionUrl = miniHS2.getJdbcURL();

    try (Connection connection = DriverManager.getConnection(connectionUrl, "user1", "pass1");
         Statement statement = connection.createStatement()) {

      statement.execute("drop materialized view if exists " + MV1);
      statement.execute("drop table if exists " + TABLE1);

      statement.execute("create table " + TABLE1 + " (a int, b int) stored by iceberg stored as orc");
      statement.execute("insert into " + TABLE1 + " values (0, 1), (2, 20)");
      statement.execute("create materialized view " + MV1 +
              " stored by iceberg stored as orc as select a, b from " + TABLE1 + " where a > 1");
      statement.execute("insert into " + TABLE1 + " values (3, 3)");
    }

    assertResults(connectionUrl, MV1, Set.of("2 20"));

    try (Connection connection = DriverManager.getConnection(connectionUrl, "user1", "pass1");
         Statement statement = connection.createStatement()) {

      statement.execute("start transaction");
      statement.execute("alter materialized view " + MV1 + " rebuild");

      try (Connection connection2 = DriverManager.getConnection(connectionUrl, "user2", "pass1");
           Statement statement2 = connection2.createStatement()) {
        statement2.execute("start transaction");
        statement2.execute("alter materialized view " + MV1 + " rebuild");
        statement2.execute("commit");
      }

      statement.execute("rollback");
    }

    assertResults(connectionUrl, MV1, Set.of("2 20"));
  }

  @Test
  public void testConcurrentMVRebuildWhenCommit() throws Exception {
    String connectionUrl = miniHS2.getJdbcURL();

    try (Connection connection = DriverManager.getConnection(connectionUrl, "user1", "pass1");
         Statement statement = connection.createStatement()) {

      statement.execute("drop materialized view if exists " + MV1);
      statement.execute("drop table if exists " + TABLE1);

      statement.execute("create table " + TABLE1 + " (a int, b int) stored by iceberg stored as orc");
      statement.execute("insert into " + TABLE1 + " values (0, 1), (2, 20)");
      statement.execute("create materialized view " + MV1 +
              " stored by iceberg stored as orc as select a, b from " + TABLE1 + " where a > 1");
      statement.execute("insert into " + TABLE1 + " values (3, 3)");
    }

    assertResults(connectionUrl, MV1, Set.of("2 20"));

    try (Connection connection = DriverManager.getConnection(connectionUrl, "user1", "pass1");
         Statement statement = connection.createStatement()) {

      statement.execute("start transaction");
      statement.execute("alter materialized view " + MV1 + " rebuild");

      try (Connection connection2 = DriverManager.getConnection(connectionUrl, "user2", "pass1");
           Statement statement2 = connection2.createStatement()) {
        statement2.execute("start transaction");
        statement2.execute("alter materialized view " + MV1 + " rebuild");
        statement2.execute("commit");
      }

      statement.execute("commit");
    }

    assertResults(connectionUrl, MV1, Set.of("2 20", "3 3"));
  }

  private void assertResults(String connectionUrl, String tableName, Set<String> expected) throws SQLException {
    try (Connection connection = DriverManager.getConnection(connectionUrl, "user1", "pass1");
         Statement statement = connection.createStatement()) {

      Set<String> expectedCopy = new HashSet<>(expected);

      ResultSet resultSet = statement.executeQuery("select * from " + tableName);
      while (resultSet.next()) {
        String line = resultSet.getInt(1) + " " + resultSet.getInt(2);
        if (!expectedCopy.remove(line)) {
          throw new AssertionError("Actual record not expected: " + line);
        }
      }
      if (!expectedCopy.isEmpty()) {
        throw new AssertionError("Expected record(s) not found: " + String.join(System.lineSeparator(), expectedCopy));
      }
    }
  }
}
