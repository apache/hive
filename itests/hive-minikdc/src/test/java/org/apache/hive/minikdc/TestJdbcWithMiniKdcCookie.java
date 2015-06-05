package org.apache.hive.minikdc;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJdbcWithMiniKdcCookie {
  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;
  private Connection hs2Conn;
  File dataFile;
  protected static HiveConf hiveConf = new HiveConf();
  private static String HIVE_NON_EXISTENT_USER = "hive_no_exist";

  @BeforeClass
  public static void beforeTest() throws Exception {
    hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, MiniHS2.HS2_HTTP_MODE);
    System.err.println("Testing using HS2 mode : "
      + hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE));
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_AUTH_ENABLED,
      true);
    // set a small time unit as cookie max age so that the server sends a 401
    hiveConf.setTimeVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_MAX_AGE,
      1, TimeUnit.SECONDS);
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_IS_SECURE, false);
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc(hiveConf);
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerb(miniHiveKdc, hiveConf);
    miniHS2.start(new HashMap<String, String>());
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      try {
        hs2Conn.close();
      } catch (Exception e) {
        // Ignore shutdown errors since there are negative tests
      }
    }
  }

  @AfterClass
  public static void afterTest() throws Exception {
    miniHS2.stop();
  }

  @Test
  public void testCookie() throws Exception {
    String tableName = "test_cookie";
    dataFile = new File(hiveConf.get("test.data.files"), "kv1.txt");
    Connection hs2Conn = getConnection(MiniHiveKdc.HIVE_TEST_USER_1);

    Statement stmt = hs2Conn.createStatement();

    // create table
    stmt.execute("create table " + tableName + "(key int, value string) ");
    stmt.execute("load data local inpath '" + dataFile + "' into table " + tableName);

    // run a query in a loop so that we hit a 401 occasionally
    for (int i = 0; i < 10; i++) {
      stmt.execute("select * from " + tableName );
    }
    stmt.execute("drop table " + tableName);
    stmt.close();
  }

  @Test
  public void testCookieNegative() throws Exception {
    try {
      // Trying to connect with a non-existent user should still fail with
      // login failure.
      getConnection(HIVE_NON_EXISTENT_USER);
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().contains("Login failure"));
    }
  }

  private Connection getConnection(String userName) throws Exception {
    miniHiveKdc.loginUser(userName);
    return new HiveConnection(miniHS2.getJdbcURL(), new Properties());
  }
}
