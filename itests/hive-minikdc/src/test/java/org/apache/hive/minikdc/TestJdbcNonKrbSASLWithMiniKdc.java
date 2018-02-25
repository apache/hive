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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.DriverManager;
import java.sql.SQLException;

import javax.security.sasl.AuthenticationException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJdbcNonKrbSASLWithMiniKdc extends TestJdbcWithMiniKdc{
  public static final String SASL_NONKRB_USER1 = "nonkrbuser";
  public static final String SASL_NONKRB_USER2 = "nonkrbuser@realm.com";
  public static final String SASL_NONKRB_PWD = "mypwd";

  public static class CustomAuthenticator implements PasswdAuthenticationProvider {
    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
      if (!(SASL_NONKRB_USER1.equals(user) && SASL_NONKRB_PWD.equals(password)) &&
          !(SASL_NONKRB_USER2.equals(user) && SASL_NONKRB_PWD.equals(password))) {
        throw new AuthenticationException("Authentication failed");
      }
    }
  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    confOverlay.put(ConfVars.HIVE_SERVER2_SESSION_HOOK.varname,
        SessionHookTest.class.getName());
    confOverlay.put(ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS.varname,
        CustomAuthenticator.class.getName());
    HiveConf hiveConf = new HiveConf();
    miniHiveKdc = MiniHiveKdc.getMiniHiveKdc(hiveConf);
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerbWithRemoteHMS(miniHiveKdc, hiveConf, "CUSTOM");
    miniHS2.start(confOverlay);
  }

  /***
   * Test a nonkrb user could login the kerberized HS2 with authentication type SASL NONE
   * @throws Exception
   */
  @Test
  public void testNonKrbSASLAuth() throws Exception {
    hs2Conn = DriverManager.getConnection(miniHS2.getBaseJdbcURL()
        + "default;user=" + SASL_NONKRB_USER1 + ";password=" + SASL_NONKRB_PWD);
    verifyProperty(SESSION_USER_NAME, SASL_NONKRB_USER1);
    hs2Conn.close();
  }

  /***
   * Test a nonkrb user could login the kerberized HS2 with authentication type SASL NONE
   * @throws Exception
   */
  @Test
  public void testNonKrbSASLFullNameAuth() throws Exception {
    hs2Conn = DriverManager.getConnection(miniHS2.getBaseJdbcURL()
        + "default;user=" + SASL_NONKRB_USER2 + ";password=" + SASL_NONKRB_PWD);
    verifyProperty(SESSION_USER_NAME, SASL_NONKRB_USER1);
    hs2Conn.close();
  }

  /***
   * Negative test, verify that connection to secure HS2 fails if it is noSasl
   * @throws Exception
   */
  @Test
  public void testNoSaslConnectionNeg() throws Exception {
    try {
      String url = miniHS2.getBaseJdbcURL() + "default;auth=noSasl";
      hs2Conn = DriverManager.getConnection(url);
      fail("noSasl connection should fail");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }
  }

  /***
   * Negative test, verify that NonKrb connection to secure HS2 fails if it is
   * user/pwd do not match.
   * @throws Exception
   */
  @Test
  public void testNoKrbConnectionNeg() throws Exception {
    try {
      String url = miniHS2.getBaseJdbcURL() + "default;user=wronguser;pwd=mypwd";
      hs2Conn = DriverManager.getConnection(url);
      fail("noSasl connection should fail");
    } catch (SQLException e) {
      // expected error
      assertEquals("08S01", e.getSQLState().trim());
    }
  }

  /***
   * Negative test for token based authentication
   * Verify that token is not applicable to non-Kerberos SASL user
   * @throws Exception
   */
  @Test
  public void testNoKrbSASLTokenAuthNeg() throws Exception {
    hs2Conn = DriverManager.getConnection(miniHS2.getBaseJdbcURL()
        + "default;user=" + SASL_NONKRB_USER1 + ";password=" + SASL_NONKRB_PWD);
    verifyProperty(SESSION_USER_NAME, SASL_NONKRB_USER1);

    try {
      // retrieve token and store in the cache
      String token = ((HiveConnection)hs2Conn).getDelegationToken(
          MiniHiveKdc.HIVE_TEST_USER_1, MiniHiveKdc.HIVE_SERVICE_PRINCIPAL);

      fail(SASL_NONKRB_USER1 + " shouldn't be allowed to retrieve token for " +
          MiniHiveKdc.HIVE_TEST_USER_2);
    } catch (SQLException e) {
      // Expected error
      assertTrue(e.getMessage().contains("Delegation token only supported over remote client with kerberos authentication"));
    } finally {
      hs2Conn.close();
    }
  }
}