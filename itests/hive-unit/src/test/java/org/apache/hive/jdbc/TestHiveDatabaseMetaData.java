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

package org.apache.hive.jdbc;

import org.apache.hive.jdbc.Utils.JdbcConnectionParams;

import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TestHiveDatabaseMetaData.
 *
 */
public class TestHiveDatabaseMetaData {

  private Map<String, String> map = new LinkedHashMap<String, String>();
  private JdbcConnectionParams jdbcConnectionParams = new JdbcConnectionParams();
  private HiveDatabaseMetaData hiveDatabaseMetaData;
  private HiveConnection connection = new HiveConnection();

  @Before
  public void setup() throws Exception {
    jdbcConnectionParams.setHiveConfs(map);
    connection.setConnParams(jdbcConnectionParams);
    hiveDatabaseMetaData = new HiveDatabaseMetaData(connection, null, null);
  }

  @Test
  public void testGetHiveDefaultNullsLastNullConfig() throws SQLException {
    map.remove(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY);
    assertTrue(hiveDatabaseMetaData.nullsAreSortedHigh());
    assertFalse(hiveDatabaseMetaData.nullsAreSortedLow());
  }

  @Test
  public void testGetHiveDefaultNullsLast() throws SQLException {
    map.put(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY, "true");
    assertTrue(hiveDatabaseMetaData.getHiveDefaultNullsLast(map));

    map.put(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY, "false");
    assertFalse(hiveDatabaseMetaData.getHiveDefaultNullsLast(map));

  }

  @Test
  public void testNullsAreSortedHigh() throws SQLException {
    map.put(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY, "false");
    assertFalse(hiveDatabaseMetaData.nullsAreSortedHigh());
    map.put(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY, "true");
    assertTrue(hiveDatabaseMetaData.nullsAreSortedHigh());
  }

  @Test
  public void testNullsAreSortedLow() throws SQLException {
    map.put(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY, "false");
    assertTrue(hiveDatabaseMetaData.nullsAreSortedLow());
    map.put(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY, "true");
    assertFalse(hiveDatabaseMetaData.nullsAreSortedLow());
  }

  @Test
  public void testHiveConnectionUdateServerHiveConf() {
    Map<String, String> serverHiveConf = new HashMap<>();
    serverHiveConf.put("hive.server2.thrift.resultset.default.fetch.size", Integer.toString(87));
    serverHiveConf.put("hive.default.nulls.last", "false");

    jdbcConnectionParams.getHiveConfs().put(Utils.JdbcConnectionParams.HIVE_CONF_PREFIX
        + "hive.server2.thrift.resultset.default.fetch.size", "1534");
    connection.updateServerHiveConf(serverHiveConf, jdbcConnectionParams);

    // Client configuration should not be overridden by the server configuration.
    assertEquals("1534", jdbcConnectionParams.getHiveConfs().get(Utils.JdbcConnectionParams.HIVE_CONF_PREFIX
        + "hive.server2.thrift.resultset.default.fetch.size"));

    // Server configuration should be updated, since its not provided by the client.
    assertEquals("false", jdbcConnectionParams.getHiveConfs()
        .get(Utils.JdbcConnectionParams.HIVE_CONF_PREFIX + "hive.default.nulls.last"));

  }

  @Test
  public void testGetUserName() throws SQLException {
    HiveConnection hiveConnection = new HiveConnection("jdbc:hive2:///;user=foo", new Properties());
    hiveDatabaseMetaData = new HiveDatabaseMetaData(hiveConnection, null, null);
    assertEquals("foo", hiveDatabaseMetaData.getUserName());
  }

  @Test
  public void testGetURL() {
    assertEquals(connection.getConnectedUrl(), hiveDatabaseMetaData.getURL());
  }
}
