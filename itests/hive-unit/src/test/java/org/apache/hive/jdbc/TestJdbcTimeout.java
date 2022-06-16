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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.DriverManager;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

public class TestJdbcTimeout {

  private static MiniHS2 miniHS2 = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager");
    miniHS2 = new MiniHS2.Builder().withConf(conf).build();
    miniHS2.start(new HashMap<>());
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
  }

  @Test
  public void testConfigureTimeoutThroughJdbcUrl() throws Exception {
    String url1 = miniHS2.getJdbcURL("default", "connectTimeout=20000");
    try (HiveConnection conn = (HiveConnection) DriverManager.getConnection(url1)) {
      assertEquals(20000, conn.getConnectTimeout());
      assertEquals(0, conn.getSocketTimeout());
    }
    String url2 = miniHS2.getJdbcURL("default", "socketTimeout=10000");
    try (HiveConnection conn = (HiveConnection) DriverManager.getConnection(url2)) {
      assertEquals(0, conn.getConnectTimeout());
      assertEquals(10000, conn.getSocketTimeout());
    }
    String url3 = miniHS2.getJdbcURL("default", "connectTimeout=20000;socketTimeout=10000");
    try (HiveConnection conn = (HiveConnection) DriverManager.getConnection(url3)) {
      assertEquals(20000, conn.getConnectTimeout());
      assertEquals(10000, conn.getSocketTimeout());
    }
  }
}
