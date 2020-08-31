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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;


import java.util.HashMap;
import java.util.Properties;
import java.util.Map;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TestHiveDatabaseMetaData.
 *
 */
public class TestHiveDatabaseMetaData {

  private static final Map<String, String> map = new HashMap<>();
  private HiveDatabaseMetaData hiveDatabaseMetaData;

  @Before
  public void setup() throws Exception {
    map.put(Utils.HIVE_CONF_PREFIX + ConfVars.HIVE_DEFAULT_NULLS_LAST, "false");
    JdbcConnectionParams jdbcConnectionParams = new JdbcConnectionParams();
    jdbcConnectionParams.setHiveConfs(map);
    HiveConnection connection = new HiveConnection();
    connection.setConnParams(jdbcConnectionParams);
    hiveDatabaseMetaData = new HiveDatabaseMetaData(connection, null, null);
  }

  @Test
  public void testGetHiveDefaultNullsLast() {
    assertFalse(HiveDatabaseMetaData.getHiveDefaultNullsLast(map));
  }

  @Test
  public void testGetHiveDefaultNullsLastDefaultValue() {
    assertTrue(HiveDatabaseMetaData.getHiveDefaultNullsLast(null));
  }

  @Test
  public void testNullsAreSortedHigh() throws SQLException {
    assertTrue(hiveDatabaseMetaData.nullsAreSortedHigh());
  }

  @Test
  public void testNullsAreSortedLow() throws SQLException {
    assertFalse(hiveDatabaseMetaData.nullsAreSortedLow());
  }

}