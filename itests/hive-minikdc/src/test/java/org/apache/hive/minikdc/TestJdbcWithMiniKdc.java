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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.session.HiveSessionHook;
import org.apache.hive.service.cli.session.HiveSessionHookContext;
import org.apache.hive.service.cli.session.SessionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestJdbcWithMiniKdc {
  // Need to hive.server2.session.hook to SessionHookTest in hive-site
  public static final String SESSION_USER_NAME = "proxy.test.session.user";

  // set current user in session conf
  public static class SessionHookTest implements HiveSessionHook {
    @Override
    public void run(HiveSessionHookContext sessionHookContext) throws HiveSQLException {
      sessionHookContext.getSessionConf().set(SESSION_USER_NAME,
          sessionHookContext.getSessionUser());
    }
  }

  protected static MiniHS2 miniHS2 = null;
  protected static MiniHiveKdc miniHiveKdc = null;
  protected static Map<String, String> confOverlay = new HashMap<String, String>();
  protected Connection hs2Conn;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    confOverlay.put(ConfVars.HIVE_SERVER2_SESSION_HOOK.varname,
        SessionHookTest.class.getName());
    confOverlay.put(ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_ENABLED.varname, "false");

    miniHiveKdc = new MiniHiveKdc();
    HiveConf hiveConf = new HiveConf();
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerb(miniHiveKdc, hiveConf);
    miniHS2.start(confOverlay);
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

  /***
   * Basic connection test
   * @throws Exception
   */
  @Test
  public void testConnection() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_USER_1);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());
    verifyProperty(SESSION_USER_NAME, MiniHiveKdc.HIVE_TEST_USER_1);
  }

  /***
   * Negative test, verify that connection to secure HS2 fails when
   * required connection attributes are not provided
   * @throws Exception
   */
  @Test
  public void testConnectionNeg() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_USER_1);
    try {
      String url = miniHS2.getJdbcURL().replaceAll(";principal.*", "");
      hs2Conn = DriverManager.getConnection(url);
      fail("NON kerberos connection should fail");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }
  }

  /***
   * Test isValid() method
   * @throws Exception
   */
  @Test
  public void testIsValid() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());
    assertTrue(hs2Conn.isValid(1000));
    hs2Conn.close();
  }

  /***
   * Negative test isValid() method
   * @throws Exception
   */
  @Test
  public void testIsValidNeg() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());
    hs2Conn.close();
    assertFalse(hs2Conn.isValid(1000));
  }

  /***
   * Test token based authentication over kerberos
   * Login as super user and retrieve the token for normal user
   * use the token to connect connect as normal user
   * @throws Exception
   */
  @Test
  public void testTokenAuth() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());

    // retrieve token and store in the cache
    String token = ((HiveConnection)hs2Conn).getDelegationToken(
        MiniHiveKdc.HIVE_TEST_USER_1, MiniHiveKdc.HIVE_SERVICE_PRINCIPAL);
    assertTrue(token != null && !token.isEmpty());
    hs2Conn.close();

    UserGroupInformation ugi = miniHiveKdc.
        loginUser(MiniHiveKdc.HIVE_TEST_USER_1);
    // Store token in the cache
    storeToken(token, ugi);
    hs2Conn = DriverManager.getConnection(miniHS2.getBaseJdbcURL() +
        "default;auth=delegationToken");
    verifyProperty(SESSION_USER_NAME, MiniHiveKdc.HIVE_TEST_USER_1);
  }

  @Test
  public void testRenewDelegationToken() throws Exception {
    UserGroupInformation currentUGI = miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());
    String currentUser = currentUGI.getUserName();
    // retrieve token and store in the cache
    String token = ((HiveConnection) hs2Conn)
        .getDelegationToken(MiniHiveKdc.HIVE_TEST_USER_1,
            miniHiveKdc.getFullyQualifiedServicePrincipal(MiniHiveKdc.HIVE_TEST_SUPER_USER));
    assertTrue(token != null && !token.isEmpty());

    ((HiveConnection) hs2Conn).renewDelegationToken(token);

    hs2Conn.close();
  }

  @Test
  public void testCancelRenewTokenFlow() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());

    // retrieve token and store in the cache
    String token = ((HiveConnection) hs2Conn)
        .getDelegationToken(MiniHiveKdc.HIVE_TEST_USER_1, MiniHiveKdc.HIVE_SERVICE_PRINCIPAL);
    assertTrue(token != null && !token.isEmpty());

    Exception ex = null;
    ((HiveConnection) hs2Conn).cancelDelegationToken(token);
    try {
      ((HiveConnection) hs2Conn).renewDelegationToken(token);
    } catch (Exception SQLException) {
      ex = SQLException;
    }
    assertTrue(ex != null && ex instanceof HiveSQLException);
    // retrieve token and store in the cache
    token = ((HiveConnection) hs2Conn)
        .getDelegationToken(MiniHiveKdc.HIVE_TEST_USER_1, MiniHiveKdc.HIVE_SERVICE_PRINCIPAL);
    assertTrue(token != null && !token.isEmpty());

    hs2Conn.close();
  }
  /***
   * Negative test for token based authentication
   * Verify that a user can't retrieve a token for user that
   * it's not allowed to impersonate
   * @throws Exception
   */
  @Test(expected = HiveSQLException.class)
  public void testNegativeTokenAuth() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL());

    try {
      // retrieve token and store in the cache
      ((HiveConnection)hs2Conn).getDelegationToken(
          MiniHiveKdc.HIVE_TEST_USER_2, MiniHiveKdc.HIVE_SERVICE_PRINCIPAL);
    } finally {
      hs2Conn.close();
    }
  }

  /**
   * Test connection using the proxy user connection property
   * @throws Exception
   */
  @Test
  public void testProxyAuth() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL("default",
        ";hive.server2.proxy.user=" + MiniHiveKdc.HIVE_TEST_USER_1));
    verifyProperty(SESSION_USER_NAME, MiniHiveKdc.HIVE_TEST_USER_1);
  }

  /**
   * Test connection using the proxy user connection property.
   * Verify proxy connection fails when super user doesn't have privilege to
   * impersonate the given user
   * @throws Exception
   */
  @Test(expected = SQLException.class)
  public void testNegativeProxyAuth() throws Exception {
    miniHiveKdc.loginUser(MiniHiveKdc.HIVE_TEST_SUPER_USER);
    hs2Conn = DriverManager
        .getConnection(miniHS2.getJdbcURL("default", ";hive.server2.proxy.user=" + MiniHiveKdc.HIVE_TEST_USER_2));
  }

  /**
   * Verify the config property value
   * @param propertyName
   * @param expectedValue
   * @throws Exception
   */
  protected void verifyProperty(String propertyName, String expectedValue) throws Exception {
    Statement stmt = hs2Conn .createStatement();
    ResultSet res = stmt.executeQuery("set " + propertyName);
    assertTrue(res.next());
    String results[] = res.getString(1).split("=");
    assertEquals("Property should be set", results.length, 2);
    assertEquals("Property should be set", expectedValue, results[1]);
  }

  // Store the given token in the UGI
  protected void storeToken(String tokenStr, UserGroupInformation ugi)
      throws Exception {
    SessionUtils.setTokenStr(ugi,
        tokenStr, HiveAuthConstants.HS2_CLIENT_TOKEN);
  }

}
