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

package org.apache.hive.service.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveDriver;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestHttpCookieAuthenticationTest.
 */
public class TestHttpCookieAuthenticationTest {
  private static MiniHS2 miniHS2;

  @BeforeClass
  public static void startServices() throws Exception {
    miniHS2 = new MiniHS2.Builder().withHTTPTransport().build();

    Map<String, String> configOverlay = new HashMap<>();
    configOverlay.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, Boolean.FALSE.toString());
    configOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_COOKIE_AUTH_ENABLED.varname, Boolean.TRUE.toString());
    miniHS2.start(configOverlay);
  }

  @AfterClass
  public static void stopServices() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
      miniHS2.cleanup();
      miniHS2 = null;
      MiniHS2.cleanupLocalDir();
    }
  }

  @Test
  public void testHttpJdbcCookies() throws Exception {
    String sqlQuery = "show tables";

    Class.forName(HiveDriver.class.getCanonicalName());

    String username = System.getProperty("user.name");
    try(Connection connection = DriverManager.getConnection(miniHS2.getJdbcURL(), username, "bar")) {
      assertNotNull(connection);

      CookieStore cookieStore = getCookieStoreFromConnection(connection);
      assertNotNull(cookieStore);

      // Test that basic cookies worked
      List<Cookie> cookies1 = cookieStore.getCookies();
      assertEquals(1, cookies1.size());

      try(Statement statement = connection.createStatement()) {
        assertNotNull(statement);
        try(ResultSet resultSet = statement.executeQuery(sqlQuery)) {
          assertNotNull(resultSet);
        }
      }

      // Check that cookies worked and still the same after a statement
      List<Cookie> cookies2 = cookieStore.getCookies();
      assertEquals(1, cookies2.size());
      assertEquals(cookies1, cookies2);

      // Empty out cookies to make sure same connection gets new cookies
      cookieStore.clear();
      assertTrue(cookieStore.getCookies().isEmpty());

      try(Statement statement = connection.createStatement()) {
        assertNotNull(statement);
        try(ResultSet resultSet = statement.executeQuery(sqlQuery)) {
          assertNotNull(resultSet);
        }
      }

      // Check that cookies worked after clearing and got back new cookie
      List<Cookie> cookies3 = cookieStore.getCookies();
      assertEquals(1, cookies3.size());
      assertNotEquals(cookies1, cookies3);


      // Get original cookie to copy metadata
      Cookie originalCookie = cookies3.get(0);

      // Put in a bad client side cookie - ensure HS2 authenticates and overwrites
      BasicClientCookie badCookie = new BasicClientCookie("hive.server2.auth", "bad");
      badCookie.setDomain(originalCookie.getDomain());
      badCookie.setPath(originalCookie.getPath());
      badCookie.setExpiryDate(originalCookie.getExpiryDate());
      cookieStore.addCookie(badCookie);

      // Check that putting in the bad cookie overrode the original cookie
      List<Cookie> cookies4 = cookieStore.getCookies();
      assertEquals(1, cookies4.size());
      assertTrue(cookies4.contains(badCookie));

      try(Statement statement = connection.createStatement()) {
        assertNotNull(statement);
        try(ResultSet resultSet = statement.executeQuery(sqlQuery)) {
          assertNotNull(resultSet);
        }
      }

      // Check that cookies worked and replaced the bad cookie
      List<Cookie> cookies5 = cookieStore.getCookies();
      assertEquals(1, cookies5.size());
      assertNotEquals(cookies4, cookies5);

      try(Statement statement = connection.createStatement()) {
        assertNotNull(statement);
        try(ResultSet resultSet = statement.executeQuery(sqlQuery)) {
          assertNotNull(resultSet);
        }
      }

      // Check that cookies worked and didn't get replaced
      List<Cookie> cookies6 = cookieStore.getCookies();
      assertEquals(1, cookies6.size());
      assertEquals(cookies5, cookies6);
    }
  }

  // ((InternalHttpClient) ((THttpClient) ((HiveConnection) connection).transport).client).cookieStore.getCookies()
  private CookieStore getCookieStoreFromConnection(Connection connection) throws Exception {
    CookieStore cookieStore = null;
    if (connection instanceof HiveConnection) {
      HiveConnection hiveConnection = (HiveConnection) connection;

      Field transportField = hiveConnection.getClass().getDeclaredField("transport");
      transportField.setAccessible(true);
      TTransport transport = (TTransport) transportField.get(hiveConnection);

      if(transport instanceof THttpClient) {
        THttpClient httpTransport = (THttpClient) transport;
        Field clientField = httpTransport.getClass().getDeclaredField("client");
        clientField.setAccessible(true);
        HttpClient httpClient = (HttpClient) clientField.get(httpTransport);

        Field cookieStoreField = httpClient.getClass().getDeclaredField("cookieStore");
        cookieStoreField.setAccessible(true);
        cookieStore = (CookieStore) cookieStoreField.get(httpClient);
      }
    }
    return cookieStore;
  }
}
