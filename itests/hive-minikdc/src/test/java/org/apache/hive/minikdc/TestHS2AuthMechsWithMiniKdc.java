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

import com.google.common.collect.ImmutableMap;

import javax.security.sasl.AuthenticationException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.auth.AuthenticationProviderFactory;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;

public class TestHS2AuthMechsWithMiniKdc {
  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniHiveKdc = null;

  public static class CustomAuthForTest implements PasswdAuthenticationProvider {
    private static List<String> authentications = new ArrayList<>();
    private static Map<String, String> validUsers =
        ImmutableMap.of("user1", "password1", "user2", "password2", "user3", "password3");
    static String error_message = "Error validating the user: %s";
    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
      authentications.add(user);
      if (validUsers.containsKey(user) && validUsers.get(user).equals(password)) {
        // noop
      } else {
        throw new AuthenticationException(String.format(error_message, user));
      }
    }
    public static String getLastAuthenticateUser() {
      return authentications.get(authentications.size() - 1);
    }
    public static int getAuthenticationSize() {
      return authentications.size();
    }
    public static void clear() {
      authentications.clear();
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    miniHiveKdc = new MiniHiveKdc();
    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS, CustomAuthForTest.class.getName());

    AuthenticationProviderFactory.AuthMethods.CUSTOM.getConf().set(HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS.varname,
        CustomAuthForTest.class.getName());
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerb(miniHiveKdc, hiveConf,
        HiveAuthConstants.AuthTypes.KERBEROS.getAuthName() + "," + HiveAuthConstants.AuthTypes.CUSTOM.getAuthName());
    miniHS2.getHiveConf().setVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE, MiniHS2.HS2_ALL_MODE);
    miniHS2.start(new HashMap<>());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    miniHS2.stop();
  }

  private void testKrbPasswordAuth(boolean httpMode) throws Exception {
    String baseJdbc, jdbc;
    if (!httpMode) {
      baseJdbc = miniHS2.getBaseJdbcURL() + "default;";
      jdbc = miniHS2.getJdbcURL();
    } else {
      baseJdbc = miniHS2.getBaseHttpJdbcURL() + "default;transportMode=http;httpPath=cliservice;";
      jdbc = miniHS2.getHttpJdbcURL();
    }

    // First we try logging through Kerberos
    try {
      String principle = miniHiveKdc.getFullyQualifiedServicePrincipal("dummy_user");
      DriverManager.getConnection(baseJdbc + "default;principal=" + principle);
      fail("Should fail to establish the connection as server principle is wrong");
    } catch (Exception e) {
      if (!httpMode) {
        Assert.assertTrue(e.getMessage().contains("GSS initiate failed"));
      } else {
        Assert.assertTrue(ExceptionUtils.getStackTrace(e).contains("Failed to find any Kerberos ticket"));
      }
    }

    try (Connection hs2Conn = DriverManager.getConnection(jdbc)) {
      try (Statement statement = hs2Conn.createStatement()) {
        statement.execute("create table if not exists test_hs2_with_multiple_auths(a string)");
        statement.execute("set hive.support.concurrency");
        validateResult(statement.getResultSet(), 1);
      }
    }

    // Next, test logging through user/password
    try {
      DriverManager.getConnection(baseJdbc + "user=user1;password=password2");
      fail("Should fail to establish the connection as password is wrong");
    } catch (Exception e) {
      if (!httpMode) {
        Assert.assertTrue(e.getMessage().contains("Error validating the login"));
      } else {
        Assert.assertTrue(e.getMessage().contains("HTTP Response code: 401"));
      }
      Assert.assertTrue(CustomAuthForTest.getAuthenticationSize() == 1);
      Assert.assertEquals("user1", CustomAuthForTest.getLastAuthenticateUser());
    }

    try (Connection hs2Conn = DriverManager.getConnection(baseJdbc + "user=user2;password=password2")) {
      try (Statement statement = hs2Conn.createStatement()) {
        statement.execute("set hive.support.concurrency");
        validateResult(statement.getResultSet(), 1);
      }
    }

    Assert.assertEquals("user2", CustomAuthForTest.getLastAuthenticateUser());
    CustomAuthForTest.clear();
  }

  @Test
  public void testKrbPasswordAuth() throws Exception {
    testKrbPasswordAuth(false); // Test the binary mode
    testKrbPasswordAuth(true);  // Test the http mode
  }

  private void validateResult(ResultSet rs, int expectedSize) throws Exception {
    int actualSize = 0;
    while (rs.next()) {
      actualSize ++;
    }
    Assert.assertEquals(expectedSize, actualSize);
  }
}
