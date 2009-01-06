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
  private HiveConf conf;
  private Path dataFilePath;
  private Connection con;
  private boolean standAloneServer = false;

  public TestJdbcDriver(String name) {
    super(name);
    conf = new HiveConf(TestJdbcDriver.class);
    String dataFileDir = conf.get("test.data.files").replace('\\', '/').replace("c:", "");
    dataFilePath = new Path(dataFileDir, "kv1.txt");
    //standAloneServer = System.getProperty("test.service.standalone.server").equals("true");
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
//    res = stmt.executeQuery("describe " + tableName);
//    while (res.next()) {
//      System.out.println(res.getString(1));
//    }

    // load data
    res = stmt.executeQuery("load data local inpath '" + dataFilePath.toString() + "' into table " + tableName);
    assertFalse(res.next());

  }

  protected void tearDown() throws Exception {
    super.tearDown();

    // drop table
    Statement stmt = con.createStatement();
    assertNotNull("Statement is null", stmt);
    ResultSet res = stmt.executeQuery("drop table " + tableName);
    assertFalse(res.next());

  }

  public final void testHiveDriver() throws Exception {
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

    while (res.next()) {
      try {
        i++;
        res.getString(2);
        res.getString(1);
        res.getInt(1);
        // TODO add assert
//      System.out.println(res.getString(2));
//      System.out.println(res.getString(1));
//      System.out.println(res.getInt(1));
      }
      catch (SQLException e) {
        System.out.println(e.toString());
        e.printStackTrace();
        throw new Exception(e.toString());
      }
    }
    // supposed to get 500 rows
    assertEquals(500, i);
  }
}
