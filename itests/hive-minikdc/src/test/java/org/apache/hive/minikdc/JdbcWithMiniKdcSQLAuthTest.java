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

package org.apache.hive.minikdc;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public abstract class JdbcWithMiniKdcSQLAuthTest {


  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;
  private Connection hs2Conn;
  protected static HiveConf hiveConf = new HiveConf();

  public static void beforeTestBase() throws Exception {
    System.err.println("Testing using HS2 mode:"
        + hiveConf.getVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE));

    Class.forName(MiniHS2.getJdbcDriverName());
    hiveConf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER,
        SQLStdHiveAuthorizerFactory.class.getName());
    hiveConf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER,
        SessionStateUserAuthenticator.class.getName());
    hiveConf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    hiveConf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);

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
  public void testAuthorization1() throws Exception {

    String tableName1 = "test_jdbc_sql_auth1";
    String tableName2 = "test_jdbc_sql_auth2";
    // using different code blocks so that jdbc variables are not accidently re-used
    // between the actions. Different connection/statement object should be used for each action.
    {
      // create tables as user1
      Connection hs2Conn = getConnection(MiniHiveKdc.HIVE_TEST_USER_1);

      Statement stmt = hs2Conn.createStatement();

      stmt.execute("drop table if exists " + tableName1);
      stmt.execute("drop table if exists " + tableName2);
      // create tables
      stmt.execute("create table " + tableName1 + "(i int) ");
      stmt.execute("create table " + tableName2 + "(i int) ");
      stmt.execute("grant select on table " + tableName2 + " to user "
          + MiniHiveKdc.HIVE_TEST_USER_2);
      stmt.close();
      hs2Conn.close();
    }

    {
      // try dropping table as user1 - should succeed
      Connection hs2Conn = getConnection((MiniHiveKdc.HIVE_TEST_USER_1));
      Statement stmt = hs2Conn.createStatement();
      stmt.execute("drop table " + tableName1);
    }

    {
      // try dropping table as user2 - should fail
      Connection hs2Conn = getConnection((MiniHiveKdc.HIVE_TEST_USER_2));
      try {
        Statement stmt = hs2Conn.createStatement();
        stmt.execute("drop table " + tableName2);
        fail("Exception due to authorization failure is expected");
      } catch (SQLException e) {
        String msg = e.getMessage();
        System.err.println("Got SQLException with message " + msg);
        // check parts of the error, not the whole string so as not to tightly
        // couple the error message with test
        assertTrue("Checking permission denied error", msg.contains("user2"));
        assertTrue("Checking permission denied error", msg.contains(tableName2));
        assertTrue("Checking permission denied error", msg.contains("OBJECT OWNERSHIP"));
      }
    }

    {
      // try reading table2 as user2 - should succeed
      Connection hs2Conn = getConnection((MiniHiveKdc.HIVE_TEST_USER_2));
      Statement stmt = hs2Conn.createStatement();
      stmt.execute(" desc " + tableName2);
    }

  }

  private Connection getConnection(String userName) throws Exception {
    miniHiveKdc.loginUser(userName);
    return DriverManager.getConnection(miniHS2.getJdbcURL());
  }



}
