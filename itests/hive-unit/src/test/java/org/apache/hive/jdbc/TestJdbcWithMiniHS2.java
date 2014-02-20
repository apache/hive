/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

  public class TestJdbcWithMiniHS2 {
    private static MiniHS2 miniHS2 = null;
    private static Path dataFilePath;

    private Connection hs2Conn = null;

    @BeforeClass
    public static void beforeTest() throws Exception {
      Class.forName(MiniHS2.getJdbcDriverName());
      HiveConf conf = new HiveConf();
      conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
      miniHS2 = new MiniHS2(conf);
      String dataFileDir = conf.get("test.data.files").replace('\\', '/')
          .replace("c:", "");
      dataFilePath = new Path(dataFileDir, "kv1.txt");
      Map<String, String> confOverlay = new HashMap<String, String>();
      miniHS2.start(confOverlay);
    }

    @Before
    public void setUp() throws Exception {
      hs2Conn = getConnection(miniHS2.getJdbcURL(), System.getProperty("user.name"), "bar");
    }

    private Connection getConnection(String jdbcURL, String user, String pwd) throws SQLException {
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
      if (miniHS2.isStarted())
        miniHS2.stop();
    }

    @Test
    public void testConnection() throws Exception {
      String tableName = "testTab1";
      Statement stmt = hs2Conn.createStatement();

      // create table
      stmt.execute("DROP TABLE IF EXISTS " + tableName);
      stmt.execute("CREATE TABLE " + tableName
          + " (under_col INT COMMENT 'the under column', value STRING) COMMENT ' test table'");

      // load data
      stmt.execute("load data local inpath '"
          + dataFilePath.toString() + "' into table " + tableName);

      ResultSet res = stmt.executeQuery("SELECT * FROM " + tableName);
      assertTrue(res.next());
      assertEquals("val_238", res.getString(2));
      res.close();
      stmt.close();
    }


    /**   This test is to connect to any database without using the command "Use <<DB>>"
     *  1)connect to default database.
     *  2) Create a new DB test_default.
     *  3) Connect to test_default database.
     *  4) Connect and create table under test_default_test.
     *  5) Connect and display all tables.
     *  6) Connect to default database and shouldn't find table test_default_test.
     *  7) Connect and drop test_default_test.
     *  8) drop test_default database.
     */

     @Test
    public void testURIDatabaseName() throws Exception{

     String  jdbcUri  = miniHS2.getJdbcURL().substring(0, miniHS2.getJdbcURL().indexOf("default"));

     hs2Conn= getConnection(jdbcUri+"default",System.getProperty("user.name"),"bar");
     String dbName="test_connection_non_default_db";
     String tableInNonDefaultSchema="table_in_non_default_schema";
     Statement stmt = hs2Conn.createStatement();
     stmt.execute("create database  if not exists "+dbName);
     stmt.close();
     hs2Conn.close();

     hs2Conn = getConnection(jdbcUri+dbName,System.getProperty("user.name"),"bar");
     stmt = hs2Conn .createStatement();
     boolean expected = stmt.execute(" create table "+tableInNonDefaultSchema +" (x int)");
     stmt.close();
     hs2Conn .close();

     hs2Conn  = getConnection(jdbcUri+dbName,System.getProperty("user.name"),"bar");
     stmt = hs2Conn .createStatement();
     ResultSet res = stmt.executeQuery("show tables");
     boolean testTableExists = false;
     while (res.next()) {
        assertNotNull("table name is null in result set", res.getString(1));
        if (tableInNonDefaultSchema.equalsIgnoreCase(res.getString(1))) {
          testTableExists = true;
        }
     }
     assertTrue("table name  "+tableInNonDefaultSchema
           + "   found in SHOW TABLES result set", testTableExists);
     stmt.close();
     hs2Conn .close();

     hs2Conn  = getConnection(jdbcUri+"default",System.getProperty("user.name"),"bar");
     stmt = hs2Conn .createStatement();
     res = stmt.executeQuery("show tables");
     testTableExists = false;
     while (res.next()) {
       assertNotNull("table name is null in result set", res.getString(1));
       if (tableInNonDefaultSchema.equalsIgnoreCase(res.getString(1))) {
         testTableExists = true;
        }
     }

     assertFalse("table name "+tableInNonDefaultSchema
           + "  NOT  found in SHOW TABLES result set", testTableExists);
     stmt.close();
     hs2Conn .close();

     hs2Conn  = getConnection(jdbcUri+dbName,System.getProperty("user.name"),"bar");
     stmt = hs2Conn .createStatement();
     stmt.execute("set hive.support.concurrency = false");
     res = stmt.executeQuery("show tables");

     stmt.execute(" drop table if exists table_in_non_default_schema");
     expected = stmt.execute("DROP DATABASE "+ dbName);
     stmt.close();

     hs2Conn  = getConnection(jdbcUri+"default",System.getProperty("user.name"),"bar");
     stmt = hs2Conn .createStatement();
     res = stmt.executeQuery("show tables");
     testTableExists = false;
     while (res.next()) {
       assertNotNull("table name is null in result set", res.getString(1));
       if (tableInNonDefaultSchema.equalsIgnoreCase(res.getString(1))) {
         testTableExists = true;
        }
     }

     // test URI with no dbName
     hs2Conn  = getConnection(jdbcUri, System.getProperty("user.name"),"bar");
     verifyCurrentDB("default", hs2Conn);
     hs2Conn.close();

     hs2Conn  = getConnection(jdbcUri + ";", System.getProperty("user.name"),"bar");
     verifyCurrentDB("default", hs2Conn);
     hs2Conn.close();

     hs2Conn  = getConnection(jdbcUri + ";/foo=bar;foo1=bar1", System.getProperty("user.name"),"bar");
     verifyCurrentDB("default", hs2Conn);
     hs2Conn.close();
     }

     @Test
     public void testConnectionSchemaAPIs() throws Exception {
       String db1 = "DB1";
       /**
        * get/set Schema are new in JDK7 and not available in java.sql.Connection in JDK6.
        * Hence the test uses HiveConnection object to call these methods so that test will run with older JDKs
        */
       HiveConnection hiveConn = (HiveConnection)hs2Conn;

       assertEquals("default", hiveConn.getSchema());
       Statement stmt = hs2Conn.createStatement();
       stmt.execute("DROP DATABASE IF EXISTS " + db1 + " CASCADE");
       stmt.execute("CREATE DATABASE " + db1);
       assertEquals("default", hiveConn.getSchema());

       stmt.execute("USE " + db1);
       assertEquals(db1, hiveConn.getSchema());

       stmt.execute("USE default");
       assertEquals("default", hiveConn.getSchema());

       hiveConn.setSchema(db1);
       assertEquals(db1, hiveConn.getSchema());
       hiveConn.setSchema("default");
       assertEquals("default", hiveConn.getSchema());

       assertTrue(hiveConn.getCatalog().isEmpty());
       hiveConn.setCatalog("foo");
       assertTrue(hiveConn.getCatalog().isEmpty());
     }

   /**
    * verify that the current db is the one expected. first create table as <db>.tab and then
    * describe that table to check if <db> is the current database
    * @param expectedDbName
    * @param hs2Conn
    * @throws Exception
    */
   private void verifyCurrentDB(String expectedDbName, Connection hs2Conn) throws Exception {
     String verifyTab = "miniHS2DbVerificationTable";
     Statement stmt = hs2Conn.createStatement();
     stmt.execute("DROP TABLE IF EXISTS " + expectedDbName + "." + verifyTab);
     stmt.execute("CREATE TABLE " + expectedDbName + "." + verifyTab + "(id INT)");
     stmt.execute("DESCRIBE " + verifyTab);
     stmt.execute("DROP TABLE IF EXISTS " + expectedDbName + "." + verifyTab);
     stmt.close();
   }
  }
