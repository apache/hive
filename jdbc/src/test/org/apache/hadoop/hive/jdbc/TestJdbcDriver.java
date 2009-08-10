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

package org.apache.hadoop.hive.jdbc;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Properties;
import junit.framework.TestCase;
import javax.naming.*;
import javax.naming.directory.*;
import javax.sql.DataSource;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;

public class TestJdbcDriver extends TestCase {
  private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static String tableName = "testHiveDriverTable";
  private static String partitionedTableName = "testHiveDriverPartitionedTable";
  private HiveConf conf;
  private Path dataFilePath;
  private Connection con;
  private boolean standAloneServer = false;

  public TestJdbcDriver(String name) {
    super(name);
    conf = new HiveConf(TestJdbcDriver.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    standAloneServer = "true".equals(System.getProperty("test.service.standalone.server"));
  }

  protected void setUp() throws Exception {
    super.setUp();
    Class.forName(driverName);
    if (standAloneServer) {
      // get connection
      con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");
    }
    else {
      con = DriverManager.getConnection("jdbc:hive://", "", "");
    }
    assertNotNull("Connection is null", con);
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    // drop table. ignore error.
    try {
      stmt.executeQuery("drop table " + tableName);
    } catch (Exception ex) {
    }

    // create table
    ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");
    assertFalse(res.next());

    // load data
    res = stmt.executeQuery("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
    assertFalse(res.next());

    // also initialize a paritioned table to test against.

    // drop table. ignore error.
    try {
      stmt.executeQuery("drop table " + partitionedTableName);
    } catch (Exception ex) {
    }

    res = stmt.executeQuery("create table " + partitionedTableName +
                            " (key int, value string) partitioned by (dt STRING)");
    assertFalse(res.next());

    // load data
    res = stmt.executeQuery("load data local inpath '" + dataFilePath.toString() +
                            "' into table " + partitionedTableName +
                            " PARTITION (dt='20090619')");
    assertFalse(res.next());

  }

  protected void tearDown() throws Exception {
    super.tearDown();

    // drop table
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);
    ResultSet res = stmt.executeQuery("drop table " + tableName);
    assertFalse(res.next());
    res = stmt.executeQuery("drop table " + partitionedTableName);
    assertFalse(res.next());

  }

  public final void testSelectAll() throws Exception {
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    ResultSet res;

    // TODO: There is no schema for show tables or describe table.
    /*
    stmt.executeQuery("drop table michi1");
    stmt.executeQuery("drop table michi2");
    stmt.executeQuery("drop table michi3");
    stmt.executeQuery("create table michi1 (num int)");
    stmt.executeQuery("create table michi2 (num int)");
    stmt.executeQuery("create table michi3 (num int)");

    res = stmt.executeQuery("show tables");
    res = stmt.executeQuery("describe michi1");
    while (res.next()) {
      System.out.println(res.getString(0));
    }
    */

    // run some queries
    res = stmt.executeQuery("select * from " + tableName);
    assertNotNull("ResultSet is null", res);
    int i = 0;

    boolean moreRow = res.next();
    while (moreRow) {
      try {
        i++;
        res.getInt(1);
        res.getString(1);
        res.getString(2);
        //System.out.println(res.getString(1) + " " + res.getString(2));
        assertEquals("getInt and getString don't align for the same result value",
                String.valueOf(res.getInt(1)), res.getString(1));
        assertEquals("Unexpected result found",
                "val_" + res.getString(1), res.getString(2));
        moreRow = res.next();
      }
      catch (SQLException e) {
        System.out.println(e.toString());
        e.printStackTrace();
        throw new Exception(e.toString());
      }
    }
    // supposed to get 500 rows
    assertEquals(500, i);

    // should have no more rows
    assertEquals(false, moreRow);
  }

  public final void testSelectAllPartitioned() throws Exception {
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    // run some queries
    ResultSet res = stmt.executeQuery("select * from " + partitionedTableName);
    assertNotNull("ResultSet is null", res);
    int i = 0;

    boolean moreRow = res.next();
    while (moreRow) {
      try {
        i++;
        res.getInt(1);
        res.getString(1);
        res.getString(2);
        //System.out.println(res.getString(1) + " " + res.getString(2));
        assertEquals("getInt and getString don't align for the same result value",
                String.valueOf(res.getInt(1)), res.getString(1));
        assertEquals("Unexpected result found",
                "val_" + res.getString(1), res.getString(2));
        moreRow = res.next();
      }
      catch (SQLException e) {
        System.out.println(e.toString());
        e.printStackTrace();
        throw new Exception(e.toString());
      }
    }
    // supposed to get 500 rows
    assertEquals(500, i);

    // should have no more rows
    assertEquals(false, moreRow);
  }

  public void testShowTables() throws SQLException {
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    ResultSet res = stmt.executeQuery("show tables");

    boolean testTableExists = false;
    while (res.next()) {
      assertNotNull("table name is null in result set", res.getString(1));
      if (tableName.equalsIgnoreCase(res.getString(1))) testTableExists = true;
    }

    assertTrue("table name " + tableName + " not found in SHOW TABLES result set",
               testTableExists);
  }

  public void testDescribeTable() throws SQLException {
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);

    ResultSet res = stmt.executeQuery("describe " + tableName);

    res.next();
    assertEquals("Column name 'key' not found", "key", res.getString(1));
    assertEquals("Column type 'int' for column key not found",
                "int", res.getString(2));
    res.next();
    assertEquals("Column name 'value' not found", "value", res.getString(1));
    assertEquals("Column type 'string' for column key not found",
                "string", res.getString(2));

    assertFalse("More results found than expected", res.next());

  }

}
