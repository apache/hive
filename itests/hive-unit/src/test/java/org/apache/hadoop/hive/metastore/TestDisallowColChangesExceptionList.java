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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test which makes sure that incompatible column changes are allowed if the serde of the
 * table is defined in the configuration metastore.allow.incompatible.col.type.changes.serdes
 */
public class TestDisallowColChangesExceptionList {

  public static MiniHS2 miniHS2 = null;

  @BeforeClass
  public static void startServices() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS, 2);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS, 2);
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    // we configure the HS2 and metastore to allow parquet and orc tables to be allowed
    // to make incompatible changes.
    hiveConf.set(MetastoreConf.ConfVars.ALLOW_INCOMPATIBLE_COL_TYPE_CHANGES_TABLE_SERDES
            .getVarname(), "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe,"
        + "org.apache.hadoop.hive.ql.io.orc.OrcSerde");

    miniHS2 = new MiniHS2.Builder().withMiniMR().withRemoteMetastore().withConf(hiveConf)
        .build();

    miniHS2.start(new HashMap<>());

    Connection hs2Conn = DriverManager
        .getConnection(miniHS2.getJdbcURL(), "hive", "hive");
    Statement stmt = hs2Conn.createStatement();

    // create test tables
    stmt.execute("drop table if exists testParquet");
    stmt.execute("create table testParquet (c1 string, c2 int) stored as parquet");
    stmt.execute("drop table if exists testOrc");
    stmt.execute("create table testOrc (c1 string, c2 int) stored as orc");
    stmt.execute("drop table if exists testText");
    stmt.execute("create table testText (c1 string, c2 int)");

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
  public void testDisallowColChangesExceptionList() {
    assertTrue("Test setup failed. MiniHS2 is not initialized",
        miniHS2 != null && miniHS2.isStarted());

    try (
        Connection hs2conn = DriverManager
            .getConnection(TestDisallowColChangesExceptionList.miniHS2.getJdbcURL(), "hive",
                "hive");
        Statement stmt = hs2conn.createStatement();
    ) {
      // parquet and orc are allowed to make incompat changes based on the
      // hiveConf
      // change c2 from int to smallint
      stmt.execute("alter table testParquet change column c2 c2 smallint");
      // drop c1
      stmt.execute("alter table testParquet replace columns (c2 int)");
      // change c1 to smallint; orc doesn't support drop columns and query fails
      // during compilation itself
      stmt.execute("alter table testOrc change column c2 c2 smallint");
      // make sure text tables don't allow incompat column changes because
      // they are not in the exception list
      try {
        // change text table column c1 from int to small; this should fail because it is
        // not in the exception list
        stmt.execute("alter table testText change column c2 c2 smallint");
        fail("Exception not thrown");
      } catch (Exception e1) {
        assertTrue("Unexpected exception: " + e1.getMessage(), e1.getMessage().contains(
            "Unable to alter table. The following columns have types incompatible with the existing columns in their respective positions"));
      }
      try {
        // drop c1 column; this should not be allowed since text is not in the exception
        // list
        stmt.execute("alter table testText replace columns (c2 int)");
        fail("Exception not thrown");
      } catch (Exception e1) {
        assertTrue("Unexpected exception: " + e1.getMessage(), e1.getMessage().contains(
            "Unable to alter table. The following columns have types incompatible with the existing columns in their respective positions"));
      }
    } catch (Exception e2) {
      fail("Unexpected Exception: " + e2.getMessage());
    }
  }

}
