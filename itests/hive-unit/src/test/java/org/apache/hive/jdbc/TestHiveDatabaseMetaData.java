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

import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.Utils;
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
  private HiveDatabaseMetaData hiveDatabaseMetaData;

  @Before
  public void setup() throws Exception {
    JdbcConnectionParams jdbcConnectionParams = new JdbcConnectionParams();
    jdbcConnectionParams.setHiveConfs(map);
    HiveConnection connection = new HiveConnection();
    connection.setConnParams(jdbcConnectionParams);
    hiveDatabaseMetaData = new HiveDatabaseMetaData(connection, null, null);
  }

  @Test
  public void testGetHiveDefaultNullsLastNullConfig() {
    map.remove(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY);
    try {
      hiveDatabaseMetaData.nullsAreSortedLow();
      fail("SQLException is expected");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("HIVE_DEFAULT_NULLS_LAST is not available"));
    }
  }

  @Test
  public void testGetHiveDefaultNullsLastNullHiveConfs() throws SQLException {
    JdbcConnectionParams jdbcConnectionParams = new JdbcConnectionParams();
    HiveConnection connection = new HiveConnection();
    connection.setConnParams(jdbcConnectionParams);
    try {
      (new HiveDatabaseMetaData(connection, null, null)).nullsAreSortedLow();
      fail("SQLException is expected");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("hiveConfs is not available"));
    }
  }

  @Test
  public void testGetHiveDefaultNullsLast() throws SQLException {
    map.put(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY, "true");
    assertTrue(hiveDatabaseMetaData.getHiveDefaultNullsLast(map));

    map.put(Utils.JdbcConnectionParams.HIVE_DEFAULT_NULLS_LAST_KEY, "false");
    assertFalse(hiveDatabaseMetaData.getHiveDefaultNullsLast(map));
  }

  @Test
  public void testGetHiveDefaultNullsLastDefaultValue() throws SQLException {
    assertTrue(HiveDatabaseMetaData.getHiveDefaultNullsLast(null));
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
    map.put("hive.server2.thrift.resultset.default.fetch.size", Integer.toString(87));
    map.put("hive.default.nulls.last", "false");
    Map<String, String> clientHiveConf = new LinkedHashMap<String, String>();
    clientHiveConf.put(Utils.JdbcConnectionParams.HIVE_CONF_PREFIX
        + "hive.server2.thrift.resultset.default.fetch.size", "1534");
    JdbcConnectionParams connectionParams = new JdbcConnectionParams();
    connectionParams.setHiveConfs(clientHiveConf);
    HiveConnection conn = new HiveConnection();
    conn.setConnParams(connectionParams);
    conn.updateServerHiveConf(serverHiveConf);

    // Server configuration should be updated, since its not provided by the client.
    assertEquals("false", conn.getConnParams().getHiveConfs()
        .get(Utils.JdbcConnectionParams.HIVE_CONF_PREFIX + "hive.default.nulls.last"));

    // Client configuration should not be overridden by the server configuration.
    assertEquals("1534", conn.getConnParams().getHiveConfs().get(Utils.JdbcConnectionParams.HIVE_CONF_PREFIX
        + "hive.server2.thrift.resultset.default.fetch.size"));
  }
}