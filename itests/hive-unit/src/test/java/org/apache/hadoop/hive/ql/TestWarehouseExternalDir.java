/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.apache.hadoop.hive.metastore.api.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.metastore.Warehouse;

public class TestWarehouseExternalDir {
  private  static final Logger LOG = LoggerFactory.getLogger(TestWarehouseExternalDir.class);

  private static MiniHS2 miniHS2;
  private static Hive db;

  private static HiveConf conf;
  private  static Connection conn;
  private  static String whRootExternal = "/wh_ext";
  private  static String dbName = "twed_db1";
  private  static Path whRootExternalPath;
  private  static Path whRootManagedPath;

  private Statement stmt;

  @BeforeClass
  public static void beforeTest() throws Exception {

    HiveConf conf = new HiveConf();

    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    conf.setVar(ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL, whRootExternal);

    miniHS2 = new MiniHS2.Builder().withMiniMR().withRemoteMetastore().withConf(conf).build();
    miniHS2.start(new HashMap<String, String>());

    FileSystem fs = miniHS2.getDfs().getFileSystem();
    whRootExternalPath = fs.makeQualified(new Path(whRootExternal));
    whRootManagedPath = fs.makeQualified(new Path(MetastoreConf.getVar(conf, MetastoreConf.ConfVars.WAREHOUSE)));

    LOG.info("fs: {}", miniHS2.getDfs().getFileSystem().getUri());
    LOG.info("warehouse location: {}", whRootManagedPath);
    LOG.info("whRootExternalPath: {}", whRootExternalPath);
    db = Hive.get(conf);
    createDb();
  }

  @AfterClass
  public  static void afterTest() throws Exception {

    if (db != null) {
      db.closeCurrent();
      db = null;
    }
    if (miniHS2 != null) {
      miniHS2.stop();
      miniHS2.cleanup();
      miniHS2 = null;
    }
    MiniHS2.cleanupLocalDir();
    Hive.closeCurrent();
    if (conn != null) {
      conn.close();
    }
  }

  @After
  public void tearDown() throws Exception {
    if (conn != null) {
      conn.close();
    }
  }

  @Before
  public  void setUp() throws Exception {
    conn = DriverManager.getConnection(miniHS2.getJdbcURL(dbName),
            System.getProperty("user.name"), "bar");
    assertNotNull(conn);
  }

  private  static void createDb() throws Exception {
    conn = DriverManager.getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    assertNotNull(conn);
    Statement stmt2 = conn.createStatement();
    stmt2.execute("DROP DATABASE IF EXISTS " + dbName + " CASCADE");
    stmt2.execute("CREATE DATABASE " + dbName);
    stmt2.close();
  }

  void checkTableLocation(Table table, Path expectedPath) throws Exception {
    LOG.info("Table {}: location {}", table.getTableName(), table.getDataLocation());
    assertEquals(table.getTableName(), expectedPath, table.getDataLocation());
    assertTrue(miniHS2.getDfs().getFileSystem().exists(table.getDataLocation()));
  }

  @Test
  public void testManagedPaths() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      // Confirm default managed table paths
      stmt.execute("create table IF NOT EXISTS default.twed_1(c1 string)");

      Table tab = db.getTable("default","twed_1");
      checkTableLocation(tab, new Path(whRootManagedPath, "twed_1"));

      stmt.execute("create table twed_db1.tab1(c1 string, c2 string)");
      tab = db.getTable("twed_db1", "tab1");
      checkTableLocation(tab, new Path(new Path(whRootManagedPath, "twed_db1.db"), "tab1"));
    }
  }

  @Test
  public void testExternalDefaultPaths() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("create external table IF NOT EXISTS default.twed_ext1(c1 string)");
      Table tab = db.getTable("default","twed_ext1");
      checkTableLocation(tab, new Path(whRootExternalPath, "twed_ext1"));

      stmt.execute("create external table twed_db1.twed_ext2(c1 string)  ");
      tab = db.getTable("twed_db1", "twed_ext2");
      checkTableLocation(tab, new Path(new Path(whRootExternalPath, "twed_db1.db"), "twed_ext2"));

      stmt.execute("create external table default.twed_ext3 like default.twed_ext1 ");
      tab = db.getTable("default", "twed_ext3");
      checkTableLocation(tab, new Path(whRootExternalPath, "twed_ext3"));

      stmt.execute("create external table twed_db1.twed_ext4 like default.twed_ext1  ");
      tab = db.getTable("twed_db1", "twed_ext4");
      checkTableLocation(tab, new Path(new Path(whRootExternalPath, "twed_db1.db"), "twed_ext4"));
    }
  }
}
